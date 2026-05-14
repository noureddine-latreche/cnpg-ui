import logging
import re
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from .config import settings

logger = logging.getLogger(__name__)

_DT_NONE = datetime.min.replace(tzinfo=timezone.utc)


class S3Client:
    def __init__(
        self,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        region: str | None = None,
    ):
        self._client = boto3.client(
            "s3",
            region_name=region or settings.AWS_REGION,
            aws_access_key_id=aws_access_key_id or settings.AWS_ACCESS_KEY_ID or None,
            aws_secret_access_key=aws_secret_access_key or settings.AWS_SECRET_ACCESS_KEY or None,
        )

    def _paginate(self, bucket: str, prefix: str) -> list[dict]:
        objects: list[dict] = []
        paginator = self._client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                objects.extend(page.get("Contents", []))
        except ClientError as e:
            logger.error("S3 list error for s3://%s/%s: %s", bucket, prefix, e)
            raise
        return objects

    # ------------------------------------------------------------------
    # backup.info parsing
    # ------------------------------------------------------------------

    def _read_backup_info(self, bucket: str, key: str) -> dict[str, str]:
        """Fetch and parse a barman backup.info file into a flat string dict."""
        try:
            resp = self._client.get_object(Bucket=bucket, Key=key)
            content = resp["Body"].read().decode("utf-8")
        except ClientError:
            return {}
        result: dict[str, str] = {}
        for line in content.splitlines():
            if "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip()
            if v.startswith("'") and v.endswith("'"):
                v = v[1:-1]
            result[k] = v
        return result

    @staticmethod
    def _parse_barman_dt(s: str) -> datetime | None:
        if not s or s in ("None", ""):
            return None
        # "2026-05-01 15:11:02.180351+00:00" → strip microseconds for fromisoformat compat
        s = re.sub(r"\.\d+", "", s)  # strip fractional seconds
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_restore_info(
        self, bucket: str, env: str, cluster_name: str = "postgres"
    ) -> dict[str, Any]:
        """
        Returns a rich dict used by the restore UI:
          backups      – list of base backups, each with timeline, times, status
          wal_coverage – per-timeline: {last_modified, segment_count}
          history_timelines – list of timeline IDs that have a .history file on S3
        """
        base_prefix = f"{env}/cnpg/{cluster_name}/base/"
        wal_prefix = f"{env}/cnpg/{cluster_name}/wals/"

        # ---- base backups ----
        try:
            base_objects = self._paginate(bucket, base_prefix)
        except ClientError:
            base_objects = []

        # group by backup folder name
        folders: dict[str, dict] = {}
        for obj in base_objects:
            rest = obj["Key"][len(base_prefix):]
            folder = rest.split("/")[0]
            if not folder:
                continue
            folders.setdefault(folder, {"name": folder, "size": 0, "s3_modified": None})
            folders[folder]["size"] += obj.get("Size", 0)
            lm = obj.get("LastModified")
            if lm and (folders[folder]["s3_modified"] is None or lm > folders[folder]["s3_modified"]):
                folders[folder]["s3_modified"] = lm

        backups: list[dict[str, Any]] = []
        for folder_name in sorted(folders):
            info = self._read_backup_info(
                bucket, f"{base_prefix}{folder_name}/backup.info"
            )
            begin_time = self._parse_barman_dt(info.get("begin_time", ""))
            end_time = self._parse_barman_dt(info.get("end_time", ""))
            backups.append({
                "name": folder_name,
                "begin_time": begin_time,
                "end_time": end_time,
                "timeline": int(info.get("timeline", "1") or "1"),
                "begin_wal": info.get("begin_wal", ""),
                "end_wal": info.get("end_wal", ""),
                "status": info.get("status", "UNKNOWN"),
                "compression": None if info.get("compression") in ("None", "", None) else info.get("compression"),
                "size": folders[folder_name]["size"],
            })

        # ---- WAL files ----
        try:
            wal_objects = self._paginate(bucket, wal_prefix)
        except ClientError:
            wal_objects = []

        wal_coverage: dict[int, dict[str, Any]] = {}
        history_timelines: list[int] = []

        for obj in wal_objects:
            filename = obj["Key"].split("/")[-1]
            lm: datetime | None = obj.get("LastModified")

            # history files
            if filename.endswith(".history"):
                try:
                    tl = int(filename.replace(".history", ""), 16)
                    if tl not in history_timelines:
                        history_timelines.append(tl)
                except ValueError:
                    pass
                continue

            # skip .backup label files
            if ".backup" in filename:
                continue

            # WAL segment — first 8 hex chars are the timeline
            if len(filename) < 8:
                continue
            try:
                tl = int(filename[:8], 16)
            except ValueError:
                continue

            if tl not in wal_coverage:
                wal_coverage[tl] = {
                    "first_segment": filename,
                    "last_modified": lm,
                    "segment_count": 1,
                }
            else:
                wal_coverage[tl]["segment_count"] += 1
                if filename < wal_coverage[tl]["first_segment"]:
                    wal_coverage[tl]["first_segment"] = filename
                if lm and (wal_coverage[tl]["last_modified"] is None or lm > wal_coverage[tl]["last_modified"]):
                    wal_coverage[tl]["last_modified"] = lm

        history_timelines.sort()

        return {
            "backups": backups,
            "wal_coverage": wal_coverage,
            "history_timelines": history_timelines,
        }

    def archive_is_empty(self, bucket: str, env: str, cluster_name: str) -> bool:
        """Return True if there are no objects under the cluster's S3 archive path."""
        prefix = f"{env}/cnpg/{cluster_name}/"
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=1):
            if page.get("Contents"):
                return False
        return True

    def delete_cluster_archive(self, bucket: str, env: str, cluster_name: str) -> int:
        """Delete every S3 object under the cluster's archive path. Returns object count."""
        prefix = f"{env}/cnpg/{cluster_name}/"
        objects = self._paginate(bucket, prefix)
        if not objects:
            return 0
        keys = [{"Key": obj["Key"]} for obj in objects]
        for i in range(0, len(keys), 1000):
            self._client.delete_objects(Bucket=bucket, Delete={"Objects": keys[i:i + 1000]})
        logger.info("Deleted %d objects from s3://%s/%s", len(keys), bucket, prefix)
        return len(keys)

    def preflight_check(
        self,
        bucket: str,
        env: str,
        cluster_name: str,
        restore_name: str,
        target_time: datetime,
    ) -> dict:
        """Verify S3 has everything needed for a PITR restore before the cluster is created.

        Checks:
          1. A DONE base backup exists before target_time
          2. WAL archive on that timeline extends past target_time
          3. Whether the restore cluster's S3 archive path is dirty (auto-cleaned on restore)

        Returns a dict with ok, errors, warnings, and diagnostic fields.
        """
        errors: list[str] = []
        warnings: list[str] = []

        info = self.get_restore_info(bucket, env, cluster_name)

        # 1 — find the best base backup
        candidates = [
            b for b in info["backups"]
            if b["status"] == "DONE" and b["end_time"] and b["end_time"] < target_time
        ]
        best = max(candidates, key=lambda b: b["end_time"]) if candidates else None
        if not best:
            errors.append(
                f"No completed base backup found before {target_time.isoformat()}. "
                f"Available backups: {[b['name'] for b in info['backups']]}"
            )

        # 2 — WAL coverage check
        wal_covers = False
        if best:
            tl = best["timeline"]
            cov = info["wal_coverage"].get(tl)
            if cov:
                last_wal_time = cov.get("last_modified")
                if last_wal_time and last_wal_time >= target_time:
                    wal_covers = True
                else:
                    errors.append(
                        f"WAL archive (timeline {tl}) last modified "
                        f"{last_wal_time.isoformat() if last_wal_time else 'unknown'} — "
                        f"does not reach target {target_time.isoformat()}. "
                        f"Run pg_switch_wal() on the primary to force archiving."
                    )
            else:
                errors.append(f"No WAL archive found for timeline {tl}.")

        # 3 — dirty archive check (warning only — cleaned automatically)
        archive_dirty = not self.archive_is_empty(bucket, env, restore_name)
        if archive_dirty:
            warnings.append(
                f"S3 archive for '{restore_name}' contains data from a previous restore "
                f"and will be cleaned automatically before starting."
            )

        return {
            "ok": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "best_backup": best["name"] if best else None,
            "best_backup_end_time": best["end_time"].isoformat() if best and best["end_time"] else None,
            "wal_covers_target": wal_covers,
            "archive_dirty": archive_dirty,
        }

    # kept for backward-compat in case anything else calls it
    def get_wal_range(
        self, bucket: str, env: str, cluster_name: str = "postgres"
    ) -> dict[str, Any]:
        info = self.get_restore_info(bucket, env, cluster_name)
        backups = info["backups"]
        oldest_base_backup = backups[0]["name"] if backups else ""
        latest_wal_time: datetime | None = None
        for cov in info["wal_coverage"].values():
            lm = cov.get("last_modified")
            if lm and (latest_wal_time is None or lm > latest_wal_time):
                latest_wal_time = lm
        return {
            "oldest_base_backup": oldest_base_backup,
            "latest_wal_time": latest_wal_time,
            "history_file_exists": len(info["history_timelines"]) > 0,
        }
