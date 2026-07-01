import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from kubernetes.client.exceptions import ApiException

from .config import settings
from .database import (
    init_db,
    get_all_settings, get_setting, set_setting,
    get_cluster_settings, set_cluster_setting, get_effective_settings,
    CLUSTER_SETTINGS_KEYS,
)
from .cnpg import CNPGClient, RESTORE_CLUSTER_NAME
from .k8s import list_contexts, get_current_context, set_context
from .s3 import S3Client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title="CNPG UI", lifespan=lifespan)

templates = Jinja2Templates(directory="app/templates")


# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------

def _fmt_ts(ts: str | datetime | None) -> str:
    """Format a timestamp string or datetime object for display."""
    if not ts:
        return "—"
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    # Try ISO 8601 from Kubernetes
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ts


def _duration(start: str | None, end: str | None) -> str:
    if not start or not end:
        return "—"
    try:
        s = datetime.fromisoformat(start.replace("Z", "+00:00"))
        e = datetime.fromisoformat(end.replace("Z", "+00:00"))
        secs = int((e - s).total_seconds())
        if secs < 60:
            return f"{secs}s"
        m, s2 = divmod(secs, 60)
        return f"{m}m {s2}s"
    except Exception:
        return "—"


def _phase_colour(phase: str) -> str:
    p = (phase or "").lower()
    if p in ("cluster in healthy state", "healthy"):
        return "green"
    if p in ("setting up", "upgrading", "switchover in progress"):
        return "yellow"
    if "error" in p or "fail" in p:
        return "red"
    return "slate"


def _backup_status_colour(status: str) -> str:
    s = (status or "").lower()
    if s == "completed":
        return "green"
    if s in ("running", "pending"):
        return "blue"
    return "red"


def _get_cnpg(db_settings: dict | None = None, namespace: str | None = None) -> CNPGClient:
    ns = namespace or (db_settings or {}).get("namespace") or settings.NAMESPACE
    return CNPGClient(namespace=ns)


async def _settings_for(cluster_name: str | None = None) -> dict:
    """Return effective settings for a (context, cluster) pair."""
    global_settings = await get_all_settings()
    name = cluster_name or global_settings.get("default_cluster", "postgres")
    context = get_current_context()
    return await get_effective_settings(context, name)


def _affinity_from_settings(db_settings: dict) -> tuple[str, dict | None, list | None]:
    """Return (storage_class, node_selector, tolerations) for cluster creation.

    storage_class comes from db_settings (user-editable) with fallback to the
    STORAGE_CLASS env var.  node_selector and tolerations come from env vars only
    (set via Helm values at deploy time) since they describe cluster topology, not
    user-level preferences.
    """
    storage_class = db_settings.get("storage_class") or settings.STORAGE_CLASS or ""
    node_selector = settings.NODE_SELECTOR or None
    tolerations = settings.TOLERATIONS or None
    return storage_class, node_selector, tolerations


def _get_s3(db_settings: dict | None = None) -> S3Client:
    return S3Client()


# ---------------------------------------------------------------------------
# Background helpers
# ---------------------------------------------------------------------------

async def _add_backup_config_when_ready(cluster_name: str, db_settings: dict) -> None:
    """Patch backup config into a running cluster once it reaches healthy state.

    Called as a background task after create_production_cluster so the cluster
    can start without triggering CNPG's 'Expected empty archive' check (which
    fires when backup is configured at creation time and the S3 path already
    contains WALs from the previous cluster of the same name).
    """
    s3_bucket = db_settings.get("s3_bucket", "")
    s3_env = db_settings.get("s3_env", "")
    aws_credentials_secret = db_settings.get("aws_credentials_secret", settings.AWS_CREDENTIALS_SECRET)
    destination_path = f"s3://{s3_bucket}/{s3_env}/cnpg" if s3_bucket and s3_env else None
    if not destination_path:
        logger.warning("No S3 config — skipping backup patch for %s", cluster_name)
        return

    try:
        cnpg = _get_cnpg(db_settings)
        # Poll up to 15 minutes for the cluster to become healthy.
        for _ in range(90):
            await asyncio.sleep(10)
            raw = cnpg.get_cluster(cluster_name)
            if raw and "healthy" in CNPGClient.cluster_phase(raw).lower():
                break
        else:
            logger.warning("Cluster %s did not reach healthy state in 15 min — backup patch skipped", cluster_name)
            return

        backup_spec = {
            "barmanObjectStore": {
                "destinationPath": destination_path,
                "s3Credentials": CNPGClient._s3_credentials(aws_credentials_secret),
                "wal": {"compression": "gzip", "maxParallel": 8},
                "data": {"compression": "gzip", "immediateCheckpoint": False, "jobs": 8},
            },
            "retentionPolicy": "30d",
        }
        cnpg._merge_patch_cluster(cluster_name, {"spec": {"backup": backup_spec}})
        logger.info("Backup config patched into %s", cluster_name)

        # Trigger an immediate base backup so a recovery point exists on the new
        # timeline right away — without this, the restore page shows no new
        # backups until the next scheduled run (which could be days away).
        # Wait briefly so CNPG's admission webhook cache reflects the patched spec;
        # without this the backup fails with "no barmanObjectStore section defined".
        await asyncio.sleep(5)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        backup_name = f"{cluster_name}-post-restore-{ts}"
        try:
            cnpg.create_backup(cluster_name, backup_name)
            logger.info("Post-restore base backup triggered: %s", backup_name)
            try:
                cnpg.switch_wal(cluster_name)
            except Exception as wal_err:
                logger.warning("pg_switch_wal after post-restore backup failed (non-fatal): %s", wal_err)
        except Exception as e:
            logger.warning("Could not trigger post-restore backup for %s: %s", cluster_name, e)
    except Exception as e:
        logger.error("Background backup patch for %s failed: %s", cluster_name, e)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Kubeconfig context switching
# ---------------------------------------------------------------------------

@app.get("/api/contexts")
async def api_list_contexts():
    return JSONResponse({
        "contexts": list_contexts(),
        "current": get_current_context(),
    })


@app.post("/api/contexts/switch")
async def api_switch_context(request: Request):
    body = await request.json()
    name = body.get("context", "").strip()
    available = list_contexts()
    if not name or (available and name not in available):
        return JSONResponse({"error": "Invalid context"}, status_code=400)
    set_context(name)
    return JSONResponse({"switched": name})


# ---------------------------------------------------------------------------
# HTML Page routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    db_settings = await _settings_for()
    clusters = []
    error_msg = None
    stats = {"total": 0, "healthy": 0, "archiving_errors": 0}

    try:
        cnpg = _get_cnpg(db_settings)
        raw_clusters = cnpg.list_clusters()
        clusters = [cnpg.cluster_summary(c) for c in raw_clusters]
        stats["total"] = len(clusters)
        stats["healthy"] = sum(
            1 for c in clusters
            if "healthy" in (c["phase"] or "").lower()
        )
        stats["archiving_errors"] = sum(
            1 for c in clusters if not c["archiving_healthy"]
        )
    except Exception as e:
        error_msg = f"Failed to connect to Kubernetes: {e}"
        logger.warning("Dashboard k8s error: %s", e)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "clusters": clusters,
            "stats": stats,
            "error_msg": error_msg,
            "namespace": db_settings.get("namespace", settings.NAMESPACE),
            "fmt_ts": _fmt_ts,
            "phase_colour": _phase_colour,
        },
    )


@app.get("/clusters/{name}", response_class=HTMLResponse)
async def cluster_detail(request: Request, name: str):
    db_settings = await _settings_for(name)
    cluster_info = None
    backups = []
    error_msg = None

    needs_post_restore_backup = False
    try:
        cnpg = _get_cnpg(db_settings)
        raw = cnpg.get_cluster(name)
        if raw:
            cluster_info = cnpg.cluster_summary(raw)
        raw_backups = cnpg.list_backups(cluster_name=name)
        raw_backups.sort(
            key=lambda b: b.get("metadata", {}).get("creationTimestamp", ""),
            reverse=True,
        )
        for b in raw_backups[:5]:
            meta = b.get("metadata", {})
            bstatus = b.get("status", {})
            backups.append({
                "name": meta.get("name", ""),
                "status": bstatus.get("phase", "Unknown"),
                "started_at": _fmt_ts(bstatus.get("startedAt")),
                "stopped_at": _fmt_ts(bstatus.get("stoppedAt")),
                "duration": _duration(bstatus.get("startedAt"), bstatus.get("stoppedAt")),
            })

        # Detect whether a post-restore base backup is needed.
        # After a restore the cluster CRD is newly created; any backup whose
        # creationTimestamp predates the cluster's own creationTimestamp belongs
        # to the previous cluster.  If no backup exists at or after the cluster's
        # creation time, the user needs to trigger one now.
        if cluster_info and cluster_info.get("archiving_healthy") and raw:
            cluster_created = raw.get("metadata", {}).get("creationTimestamp", "")
            has_backup_since_creation = any(
                b.get("metadata", {}).get("creationTimestamp", "") >= cluster_created
                for b in raw_backups
            )
            needs_post_restore_backup = not has_backup_since_creation
    except Exception as e:
        error_msg = f"Error: {e}"
        logger.warning("Cluster detail error for %s: %s", name, e)

    return templates.TemplateResponse(
        "cluster.html",
        {
            "request": request,
            "cluster": cluster_info,
            "cluster_name": name,
            "backups": backups,
            "needs_post_restore_backup": needs_post_restore_backup,
            "error_msg": error_msg,
            "namespace": db_settings.get("namespace", settings.NAMESPACE),
            "fmt_ts": _fmt_ts,
            "phase_colour": _phase_colour,
            "backup_status_colour": _backup_status_colour,
        },
    )


@app.get("/backups", response_class=HTMLResponse)
async def backups_page(request: Request):
    db_settings = await _settings_for()
    backups = []
    clusters = []
    error_msg = None

    try:
        cnpg = _get_cnpg(db_settings)
        raw_clusters = cnpg.list_clusters()
        clusters = [c.get("metadata", {}).get("name", "") for c in raw_clusters]

        raw_backups = cnpg.list_backups()
        raw_backups.sort(
            key=lambda b: b.get("metadata", {}).get("creationTimestamp", ""),
            reverse=True,
        )
        for b in raw_backups:
            meta = b.get("metadata", {})
            bstatus = b.get("status", {})
            backups.append({
                "name": meta.get("name", ""),
                "cluster": b.get("spec", {}).get("cluster", {}).get("name", ""),
                "status": bstatus.get("phase", "Unknown"),
                "started_at": _fmt_ts(bstatus.get("startedAt")),
                "stopped_at": _fmt_ts(bstatus.get("stoppedAt")),
                "duration": _duration(bstatus.get("startedAt"), bstatus.get("stoppedAt")),
            })
    except Exception as e:
        error_msg = f"Error: {e}"
        logger.warning("Backups page error: %s", e)

    return templates.TemplateResponse(
        "backups.html",
        {
            "request": request,
            "backups": backups,
            "clusters": clusters,
            "default_cluster": db_settings.get("default_cluster", settings.DEFAULT_CLUSTER),
            "error_msg": error_msg,
            "namespace": db_settings.get("namespace", settings.NAMESPACE),
            "backup_status_colour": _backup_status_colour,
        },
    )


@app.get("/restore", response_class=HTMLResponse)
async def restore_page(request: Request):
    db_settings = await _settings_for()
    restore_cluster = None
    clusters = []
    error_msg = None

    try:
        cnpg = _get_cnpg(db_settings)
        raw_clusters = cnpg.list_clusters()
        clusters = [c.get("metadata", {}).get("name", "") for c in raw_clusters]
        restore_name = db_settings.get("restore_cluster_name", RESTORE_CLUSTER_NAME)
        raw_restore = cnpg.get_restore_cluster(restore_name)
        if raw_restore:
            restore_cluster = cnpg.cluster_summary(raw_restore)
    except Exception as e:
        error_msg = f"Error connecting to Kubernetes: {e}"
        logger.warning("Restore page error: %s", e)

    return templates.TemplateResponse(
        "restore.html",
        {
            "request": request,
            "restore_cluster": restore_cluster,
            "clusters": clusters,
            "default_cluster": db_settings.get("default_cluster", settings.DEFAULT_CLUSTER),
            "s3_bucket": db_settings.get("s3_bucket", settings.S3_BUCKET),
            "s3_env": db_settings.get("s3_env", ""),
            "error_msg": error_msg,
            "namespace": db_settings.get("namespace", settings.NAMESPACE),
            "phase_colour": _phase_colour,
        },
    )


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, cluster: str = "", saved: str = ""):
    global_settings = await get_all_settings()
    editing_cluster = cluster.strip() or global_settings.get("default_cluster", "postgres")

    clusters: list[str] = []
    try:
        cnpg = _get_cnpg(global_settings)
        clusters = [c.get("metadata", {}).get("name", "") for c in cnpg.list_clusters()]
    except Exception:
        pass
    if editing_cluster not in clusters:
        clusters = [editing_cluster] + [c for c in clusters if c != editing_cluster]

    effective = await get_effective_settings(get_current_context(), editing_cluster)
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "settings": effective,
            "editing_cluster": editing_cluster,
            "editing_context": get_current_context(),
            "clusters": clusters,
            "saved": saved == "1",
            "namespace": effective.get("namespace", settings.NAMESPACE),
        },
    )


@app.post("/settings", response_class=HTMLResponse)
async def save_settings(
    request: Request,
    editing_cluster: str = Form("postgres"),
    namespace: str = Form("default"),
    aws_region: str = Form("us-east-1"),
    default_cluster: str = Form("postgres"),
    s3_bucket: str = Form(""),
    s3_env: str = Form(""),
    restore_cluster_name: str = Form("postgres-restore"),
    aws_credentials_secret: str = Form("aws-credentials"),
    storage_size: str = Form("100Gi"),
    wal_storage_size: str = Form("20Gi"),
    storage_class: str = Form(""),
    app_owner: str = Form("app"),
    app_database: str = Form("app"),
):
    cluster_name = editing_cluster.strip() or default_cluster.strip()

    # Global settings
    await set_setting("namespace", namespace.strip())
    await set_setting("aws_region", aws_region.strip())
    await set_setting("default_cluster", default_cluster.strip())

    # Per-cluster settings
    per_cluster = {
        "s3_bucket": s3_bucket.strip(),
        "s3_env": s3_env.strip(),
        "restore_cluster_name": restore_cluster_name.strip(),
        "aws_credentials_secret": aws_credentials_secret.strip(),
        "storage_size": storage_size.strip(),
        "wal_storage_size": wal_storage_size.strip(),
        "storage_class": storage_class.strip(),
        "app_owner": app_owner.strip(),
        "app_database": app_database.strip(),
    }
    context = get_current_context()
    for key, value in per_cluster.items():
        await set_cluster_setting(context, cluster_name, key, value)

    return RedirectResponse(url=f"/settings?cluster={cluster_name}&saved=1", status_code=303)


@app.patch("/api/settings")
async def api_patch_settings(request: Request):
    """Persist a subset of settings from JS (e.g. s3_bucket + s3_env from restore page).

    Pass `cluster` in the body to scope per-cluster keys to a specific cluster.
    Global keys (namespace, aws_region, default_cluster) are always stored globally.
    """
    _global = {"default_cluster", "namespace", "aws_region"}
    _per_cluster = {"s3_bucket", "s3_env", "aws_credentials_secret", "storage_size", "wal_storage_size", "storage_class", "app_owner", "app_database", "restore_cluster_name"}
    body = await request.json()
    cluster_name = body.get("cluster", "").strip()
    if not cluster_name:
        global_settings = await get_all_settings()
        cluster_name = global_settings.get("default_cluster", "postgres")
    saved = []
    for key, value in body.items():
        if not isinstance(value, str) or key == "cluster":
            continue
        if key in _global:
            await set_setting(key, value.strip())
            saved.append(key)
        elif key in _per_cluster:
            await set_cluster_setting(get_current_context(), cluster_name, key, value.strip())
            saved.append(key)
    return JSONResponse({"saved": saved})


# ---------------------------------------------------------------------------
# JSON API routes
# ---------------------------------------------------------------------------

@app.get("/api/clusters")
async def api_list_clusters():
    db_settings = await _settings_for()
    try:
        cnpg = _get_cnpg(db_settings)
        raw_clusters = cnpg.list_clusters()
        return JSONResponse([cnpg.cluster_summary(c) for c in raw_clusters])
    except Exception as e:
        logger.error("API list clusters error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/clusters/{name}")
async def api_get_cluster(name: str):
    db_settings = await _settings_for()
    try:
        cnpg = _get_cnpg(db_settings)
        raw = cnpg.get_cluster(name)
        if not raw:
            return JSONResponse({"error": "Not found"}, status_code=404)
        summary = cnpg.cluster_summary(raw)
        return JSONResponse(summary)
    except Exception as e:
        logger.error("API get cluster %s error: %s", name, e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/backups")
async def api_list_backups(cluster: str = Query(default="")):
    db_settings = await _settings_for()
    try:
        cnpg = _get_cnpg(db_settings)
        raw_backups = cnpg.list_backups(cluster_name=cluster or None)
        result = []
        for b in raw_backups:
            meta = b.get("metadata", {})
            bstatus = b.get("status", {})
            result.append({
                "name": meta.get("name", ""),
                "cluster": b.get("spec", {}).get("cluster", {}).get("name", ""),
                "status": bstatus.get("phase", "Unknown"),
                "started_at": bstatus.get("startedAt"),
                "stopped_at": bstatus.get("stoppedAt"),
                "duration": _duration(bstatus.get("startedAt"), bstatus.get("stoppedAt")),
                "created_at": meta.get("creationTimestamp"),
            })
        result.sort(key=lambda x: x.get("created_at") or "", reverse=True)
        return JSONResponse(result)
    except Exception as e:
        logger.error("API list backups error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/backups")
async def api_create_backup(request: Request):
    db_settings = await _settings_for()
    body = await request.json()
    cluster_name = body.get("cluster_name", "")
    backup_name = body.get("backup_name", "")
    if not cluster_name or not backup_name:
        return JSONResponse({"error": "cluster_name and backup_name are required"}, status_code=400)
    try:
        cnpg = _get_cnpg(db_settings)
        result = cnpg.create_backup(cluster_name, backup_name)
        # Force the current WAL segment to archive immediately so the backup's end WAL
        # is in S3 before any restore is attempted.
        try:
            wal_pos = cnpg.switch_wal(cluster_name)
            logger.info("WAL switched after backup creation: %s", wal_pos)
        except Exception as wal_err:
            logger.warning("pg_switch_wal after backup failed (non-fatal): %s", wal_err)
        return JSONResponse({"name": result.get("metadata", {}).get("name", backup_name)})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        logger.error("API create backup error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/backups/{name}")
async def api_delete_backup(name: str):
    db_settings = await _settings_for()
    try:
        cnpg = _get_cnpg(db_settings)
        cnpg.delete_backup(name)
        return JSONResponse({"deleted": name})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        logger.error("API delete backup %s error: %s", name, e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/restore/preflight")
async def api_restore_preflight(
    target_time: str = Query(default=""),
    cluster: str = Query(default=""),
    bucket: str = Query(default=""),
    env: str = Query(default=""),
):
    """Pre-flight check: verify S3 has a base backup and WAL coverage for the target time."""
    db_settings = await _settings_for()
    use_bucket = bucket or db_settings.get("s3_bucket", "")
    use_cluster = cluster or db_settings.get("default_cluster", "postgres")
    use_env = env or db_settings.get("s3_env", "")
    restore_name = db_settings.get("restore_cluster_name", RESTORE_CLUSTER_NAME)

    if not use_bucket or not use_env or not target_time:
        return JSONResponse({"error": "bucket, env, and target_time are required"}, status_code=400)
    try:
        from datetime import datetime as dt
        parsed = dt.fromisoformat(target_time.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            from datetime import timezone
            parsed = parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return JSONResponse({"error": f"Invalid target_time format: {target_time}"}, status_code=400)
    try:
        s3 = _get_s3(db_settings)
        result = s3.preflight_check(use_bucket, use_env, use_cluster, restore_name, parsed)
        return JSONResponse(result)
    except Exception as e:
        logger.error("Preflight check error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/restore/range")
async def api_restore_range(
    cluster: str = Query(default="postgres"),
    bucket: str = Query(default=""),
    env: str = Query(default=""),
):
    db_settings = await _settings_for()
    use_bucket = bucket or db_settings.get("s3_bucket", "")
    use_cluster = cluster or db_settings.get("default_cluster", "postgres")
    if not use_bucket or not env:
        return JSONResponse({"error": "bucket and env are required"}, status_code=400)
    try:
        s3 = _get_s3(db_settings)
        info = s3.get_restore_info(use_bucket, env, use_cluster)

        # Serialize backups (convert datetimes → ISO strings)
        backups_out = []
        for b in info["backups"]:
            backups_out.append({
                "name": b["name"],
                "begin_time": b["begin_time"].isoformat() if b["begin_time"] else None,
                "end_time": b["end_time"].isoformat() if b["end_time"] else None,
                "timeline": b["timeline"],
                "end_wal": b["end_wal"],
                "status": b["status"],
                "size": b["size"],
                "compression": b["compression"],
            })

        # Serialize WAL coverage
        wal_coverage_out = {}
        for tl, cov in info["wal_coverage"].items():
            lm = cov.get("last_modified")
            wal_coverage_out[str(tl)] = {
                "first_segment": cov.get("first_segment", ""),
                "last_modified": lm.isoformat() if lm else None,
                "segment_count": cov["segment_count"],
            }

        return JSONResponse({
            "backups": backups_out,
            "wal_coverage": wal_coverage_out,
            "history_timelines": info["history_timelines"],
        })
    except Exception as e:
        logger.error("API restore range error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/restore/status")
async def api_restore_status():
    db_settings = await _settings_for()
    restore_name = db_settings.get("restore_cluster_name", RESTORE_CLUSTER_NAME)
    try:
        cnpg = _get_cnpg(db_settings)
        raw = cnpg.get_restore_cluster(restore_name)
        if not raw:
            return JSONResponse({"exists": False})
        summary = cnpg.cluster_summary(raw)
        summary["exists"] = True
        return JSONResponse(summary)
    except Exception as e:
        logger.error("API restore status error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/restore")
async def api_create_restore(request: Request):
    db_settings = await _settings_for()
    body = await request.json()
    target_time = body.get("target_time", "")
    source_cluster = body.get("source_cluster", "")
    destination_path = body.get("destination_path", "")
    restore_name = db_settings.get("restore_cluster_name", RESTORE_CLUSTER_NAME)
    aws_credentials_secret = db_settings.get("aws_credentials_secret", settings.AWS_CREDENTIALS_SECRET)
    storage_size = db_settings.get("storage_size", "100Gi")
    wal_storage_size = db_settings.get("wal_storage_size", "20Gi")

    if not target_time or not source_cluster or not destination_path:
        return JSONResponse(
            {"error": "target_time, source_cluster, and destination_path are required"},
            status_code=400,
        )
    try:
        # Pre-flight: verify S3 has a base backup and WAL coverage, and clean any
        # leftover archive from a previous restore so CNPG's empty-archive check passes.
        s3_bucket = db_settings.get("s3_bucket", "")
        s3_env = db_settings.get("s3_env", "")
        source_cluster_name = db_settings.get("default_cluster", source_cluster)
        if s3_bucket and s3_env and target_time:
            try:
                from datetime import datetime as dt, timezone
                parsed_target = dt.fromisoformat(target_time.replace("Z", "+00:00"))
                if parsed_target.tzinfo is None:
                    parsed_target = parsed_target.replace(tzinfo=timezone.utc)
                s3 = _get_s3(db_settings)
                check = s3.preflight_check(s3_bucket, s3_env, source_cluster_name, restore_name, parsed_target)
                if not check["ok"]:
                    return JSONResponse({"error": "Pre-flight check failed", "details": check}, status_code=400)
                if check["archive_dirty"]:
                    deleted = s3.delete_cluster_archive(s3_bucket, s3_env, restore_name)
                    logger.info("Cleaned %d objects from restore archive before starting", deleted)
            except Exception as preflight_err:
                logger.warning("Pre-flight check failed (non-fatal, proceeding): %s", preflight_err)

        storage_class, node_selector, tolerations = _affinity_from_settings(db_settings)
        cnpg = _get_cnpg(db_settings)
        cnpg.create_restore_cluster(
            target_time=target_time,
            source_cluster=source_cluster,
            destination_path=destination_path,
            aws_credentials_secret=aws_credentials_secret,
            restore_name=restore_name,
            storage_size=storage_size,
            wal_storage_size=wal_storage_size,
            storage_class=storage_class,
            node_selector=node_selector,
            tolerations=tolerations,
        )
        return JSONResponse({"created": restore_name})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        logger.error("API create restore error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/promote/status")
async def api_promote_status():
    db_settings = await _settings_for()
    original = db_settings.get("default_cluster", settings.DEFAULT_CLUSTER)
    service_name = original  # app connects to the bare service name, not -rw
    result: dict[str, Any] = {
        "original_cluster": {"exists": False, "name": original},
        "service": {"name": service_name, "cluster_selector": None},
        "restore_cluster": None,
        "service_name": service_name,
    }
    try:
        cnpg = _get_cnpg(db_settings)
        raw = cnpg.get_cluster(original)
        if raw:
            s = cnpg.cluster_summary(raw)
            result["original_cluster"] = {
                "exists": True,
                "name": original,
                "phase": s["phase"],
                "hibernated": s["hibernated"],
                "ready_instances": s["ready_instances"],
            }
    except Exception as e:
        logger.warning("promote/status: cluster lookup failed: %s", e)

    try:
        cnpg = _get_cnpg(db_settings)
        svc = cnpg.get_service_info(service_name)
        if svc:
            result["service"] = svc
    except Exception as e:
        logger.warning("promote/status: service lookup failed: %s", e)

    restore_name = db_settings.get("restore_cluster_name", RESTORE_CLUSTER_NAME)
    try:
        cnpg = _get_cnpg(db_settings)
        raw_restore = cnpg.get_restore_cluster(restore_name)
        if raw_restore:
            result["restore_cluster"] = cnpg.cluster_summary(raw_restore)
    except Exception as e:
        logger.warning("promote/status: restore cluster lookup failed: %s", e)

    return JSONResponse(result)


@app.post("/api/promote/hibernate")
async def api_promote_hibernate():
    db_settings = await _settings_for()
    original = db_settings.get("default_cluster", settings.DEFAULT_CLUSTER)
    try:
        cnpg = _get_cnpg(db_settings)
        cnpg.hibernate_cluster(original)
        return JSONResponse({"hibernated": original})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/promote/unhibernate")
async def api_promote_unhibernate():
    db_settings = await _settings_for()
    original = db_settings.get("default_cluster", settings.DEFAULT_CLUSTER)
    try:
        cnpg = _get_cnpg(db_settings)
        cnpg.unhibernate_cluster(original)
        return JSONResponse({"unhibernated": original})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/promote/switch")
async def api_promote_switch():
    db_settings = await _settings_for()
    original = db_settings.get("default_cluster", settings.DEFAULT_CLUSTER)
    try:
        cnpg = _get_cnpg(db_settings)
        # Patch both the bare service (used by most app configs) and the -rw service
        restore_name = db_settings.get("restore_cluster_name", RESTORE_CLUSTER_NAME)
        for svc in [original, f"{original}-rw"]:
            try:
                cnpg.patch_service_cluster_selector(svc, restore_name)
            except ApiException as e:
                if e.status != 404:
                    raise
        return JSONResponse({"switched": [original, f"{original}-rw"], "to": restore_name})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/promote/unswitch")
async def api_promote_unswitch():
    db_settings = await _settings_for()
    original = db_settings.get("default_cluster", settings.DEFAULT_CLUSTER)
    try:
        cnpg = _get_cnpg(db_settings)
        for svc in [original, f"{original}-rw"]:
            try:
                cnpg.patch_service_cluster_selector(svc, original)
            except ApiException as e:
                if e.status != 404:
                    raise
        return JSONResponse({"switched": [original, f"{original}-rw"], "to": original})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/promote/cleanup")
async def api_promote_cleanup(request: Request):
    db_settings = await _settings_for()
    original = db_settings.get("default_cluster", settings.DEFAULT_CLUSTER)
    body = await request.json()
    if body.get("confirm") != original:
        return JSONResponse({"error": f"Confirmation text must be '{original}'"}, status_code=400)
    try:
        cnpg = _get_cnpg(db_settings)
        raw = cnpg.get_cluster(original)
        if raw and not CNPGClient.is_hibernated(raw):
            return JSONResponse({"error": "Cluster is not hibernated — hibernate it first"}, status_code=400)

        # Capture postgresql params, resources, and image before deletion so
        # create_production_cluster can restore the original tuning profile.
        if raw:
            spec = raw.get("spec", {})
            saved = {
                "postgresql": spec.get("postgresql"),
                "resources": spec.get("resources"),
                "imageName": spec.get("imageName"),
            }
            await set_setting("_saved_postgres_spec", json.dumps(saved))
            logger.info("Saved postgres spec before deletion")

        # Orphan PVCs before deleting CRD so Kubernetes GC doesn't take them.
        preserved_pvcs = cnpg.detach_cluster_pvcs(original)
        cnpg.delete_cluster(original)
        return JSONResponse({"deleted": original, "preserved_pvcs": preserved_pvcs})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/promote/original-pvcs")
async def api_get_original_pvcs():
    db_settings = await _settings_for()
    original = db_settings.get("default_cluster", settings.DEFAULT_CLUSTER)
    try:
        cnpg = _get_cnpg(db_settings)
        pvcs = cnpg.list_cluster_pvcs(original)
        return JSONResponse({"pvcs": pvcs, "cluster": original})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/promote/original-pvcs")
async def api_delete_original_pvcs():
    db_settings = await _settings_for()
    original = db_settings.get("default_cluster", settings.DEFAULT_CLUSTER)
    try:
        cnpg = _get_cnpg(db_settings)
        pvcs = cnpg.list_cluster_pvcs(original)
        cnpg.delete_named_pvcs(pvcs)
        return JSONResponse({"deleted": pvcs})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/promote/finalize")
async def api_promote_finalize():
    """Create the production-named cluster directly from S3 PITR.

    Reads the target_time from the restore cluster's bootstrap spec so the new
    production cluster recovers to the exact same point. Backup config is added
    automatically by a background task once the cluster reaches healthy state —
    this avoids CNPG's 'Expected empty archive' check which fires when backup is
    configured at creation time and the S3 path already has WALs.
    """
    db_settings = await _settings_for()
    original = db_settings.get("default_cluster", settings.DEFAULT_CLUSTER)
    restore_name = db_settings.get("restore_cluster_name", RESTORE_CLUSTER_NAME)
    try:
        cnpg = _get_cnpg(db_settings)
        restore_cluster_raw = cnpg.get_cluster(restore_name)
        if not restore_cluster_raw:
            return JSONResponse({"error": f"Restore cluster '{restore_name}' not found"}, status_code=404)
        if CNPGClient.is_hibernated(restore_cluster_raw):
            return JSONResponse(
                {"error": f"'{restore_name}' is hibernated — wake it before finalizing"},
                status_code=400,
            )

        # Read the target_time that was used to create the restore cluster.
        target_time = (
            restore_cluster_raw.get("spec", {})
            .get("bootstrap", {})
            .get("recovery", {})
            .get("recoveryTarget", {})
            .get("targetTime", "")
        )
        if not target_time:
            return JSONResponse(
                {"error": "Could not read targetTime from restore cluster bootstrap spec"},
                status_code=400,
            )

        s3_bucket = db_settings.get("s3_bucket", "")
        s3_env = db_settings.get("s3_env", "")
        if not s3_bucket or not s3_env:
            return JSONResponse(
                {"error": "s3_bucket and s3_env must be configured in Settings before finalizing"},
                status_code=400,
            )
        destination_path = f"s3://{s3_bucket}/{s3_env}/cnpg"
        aws_credentials_secret = db_settings.get("aws_credentials_secret", settings.AWS_CREDENTIALS_SECRET)
        source_cluster_s3_name = db_settings.get("default_cluster", "postgres")
        app_owner = db_settings.get("app_owner", "app")
        app_database = db_settings.get("app_database", "app")

        # Load postgresql/resources/image captured by cleanup step.
        saved_spec: dict = {}
        saved_spec_json = await get_setting("_saved_postgres_spec", "")
        if saved_spec_json:
            try:
                saved_spec = json.loads(saved_spec_json)
            except ValueError:
                pass

        storage_class, node_selector, tolerations = _affinity_from_settings(db_settings)
        cnpg.create_production_cluster(
            cluster_name=original,
            target_time=target_time,
            source_cluster_s3_name=source_cluster_s3_name,
            destination_path=destination_path,
            aws_credentials_secret=aws_credentials_secret,
            storage_size=db_settings.get("storage_size", "100Gi"),
            wal_storage_size=db_settings.get("wal_storage_size", "20Gi"),
            storage_class=storage_class,
            node_selector=node_selector,
            tolerations=tolerations,
            app_owner=app_owner,
            app_database=app_database,
            saved_postgresql_spec=saved_spec.get("postgresql"),
            saved_resources_spec=saved_spec.get("resources"),
            image_name=saved_spec.get("imageName"),
        )

        # Patch in backup config once the cluster is healthy.
        asyncio.create_task(
            _add_backup_config_when_ready(original, dict(db_settings))
        )

        return JSONResponse({
            "created": original,
            "method": "s3_recovery",
            "target_time": target_time,
            "backup_config": "pending",
            "post_restore_backup": "pending",
        })
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        logger.error("API promote finalize error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/clusters/{name}/enable-backup")
async def api_enable_backup(name: str, trigger_backup: bool = False):
    """Patch the full backup/WAL-archiving spec into a running cluster.

    Safe to call on a cluster that already has backup configured — the patch is idempotent.
    When trigger_backup=true, also creates an immediate base backup after patching.  A 5-second
    pause is inserted between the patch and backup creation so CNPG's admission webhook cache
    has time to reflect the new spec — without it the backup fails with "no barmanObjectStore
    section defined" even though the patch succeeded.
    """
    db_settings = await _settings_for(name)
    s3_bucket = db_settings.get("s3_bucket", "")
    s3_env = db_settings.get("s3_env", "")
    if not s3_bucket or not s3_env:
        return JSONResponse(
            {"error": "s3_bucket and s3_env must be configured in Settings first"},
            status_code=400,
        )
    aws_credentials_secret = db_settings.get("aws_credentials_secret", settings.AWS_CREDENTIALS_SECRET)
    destination_path = f"s3://{s3_bucket}/{s3_env}/cnpg"
    try:
        cnpg = _get_cnpg(db_settings)
        raw = cnpg.get_cluster(name)
        if not raw:
            return JSONResponse({"error": f"Cluster '{name}' not found"}, status_code=404)

        backup_spec = {
            "barmanObjectStore": {
                "destinationPath": destination_path,
                "s3Credentials": CNPGClient._s3_credentials(aws_credentials_secret),
                "wal": {"compression": "gzip", "maxParallel": 8},
                "data": {"compression": "gzip", "immediateCheckpoint": False, "jobs": 8},
            },
            "retentionPolicy": "30d",
        }
        cnpg._merge_patch_cluster(name, {"spec": {"backup": backup_spec}})
        logger.info("Backup config patched into %s", name)

        try:
            wal_pos = cnpg.switch_wal(name)
            logger.info("WAL switched after enabling archiving: %s", wal_pos)
        except Exception as wal_err:
            logger.warning("pg_switch_wal after enable-backup failed (non-fatal): %s", wal_err)
            wal_pos = None

        backup_triggered = None
        if trigger_backup:
            # Wait for CNPG's admission webhook informer cache to reflect the patched
            # spec before creating the Backup CRD — without this pause CNPG validates
            # against the stale (pre-patch) cluster spec and rejects with "no
            # barmanObjectStore section defined on the target cluster".
            await asyncio.sleep(5)
            ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            backup_name = f"{name}-post-restore-{ts}"
            try:
                cnpg.create_backup(name, backup_name)
                backup_triggered = backup_name
                logger.info("Post-restore base backup triggered: %s", backup_name)
            except ApiException as be:
                logger.warning("Post-restore backup creation failed: %s", be)
                backup_triggered = f"error: {getattr(be, 'body', None) or be.reason}"

        return JSONResponse({
            "cluster": name,
            "backup_patched": True,
            "wal_switched": wal_pos,
            "backup_triggered": backup_triggered,
        })
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        logger.error("enable-backup error for %s: %s", name, e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/restore")
async def api_delete_restore():
    db_settings = await _settings_for()
    restore_name = db_settings.get("restore_cluster_name", RESTORE_CLUSTER_NAME)
    try:
        cnpg = _get_cnpg(db_settings)
        cnpg.delete_restore_cluster(restore_name)
        return JSONResponse({"deleted": restore_name})
    except ApiException as e:
        return JSONResponse({"error": getattr(e, 'body', None) or e.reason or str(e)}, status_code=e.status or 500)
    except Exception as e:
        logger.error("API delete restore error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)
