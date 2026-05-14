# CNPG UI — Helm Chart

A web interface for [CloudNativePG](https://cloudnative-pg.io/) that covers the full PostgreSQL lifecycle: cluster provisioning, scheduled backups, and point-in-time recovery (PITR).

## Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
  - [Fresh cluster](#option-a-fresh-postgresql-cluster)
  - [Existing cluster](#option-b-existing-cluster-ui-only)
- [Configuration Reference](#configuration-reference)
- [Backup & Restore Workflow](#backup--restore-workflow)
- [Custom PostgreSQL Parameters](#custom-postgresql-parameters)
- [Node Affinity & Tolerations](#node-affinity--tolerations)
- [Ingress & TLS](#ingress--tls)
- [Upgrading](#upgrading)
- [Uninstalling](#uninstalling)

---

## Prerequisites

| Requirement | Version |
|---|---|
| Kubernetes | 1.25+ |
| Helm | 3.10+ |
| [CloudNativePG operator](https://cloudnative-pg.io/docs/installation/) | 1.22+ — bundled, no separate install needed |
| AWS S3 bucket (for backups) | — |

### Create the AWS credentials secret

CNPG reads S3 credentials from a Kubernetes Secret. Create it in the namespace where your cluster will live (default: `default`):

```bash
kubectl create secret generic aws-credentials \
  --from-literal=access-key=******* \
  --from-literal=secret-key=******* \
  --namespace default
```

---

## Quick Start

### Option A — Fresh PostgreSQL cluster

Use this when the namespace has no existing cluster. The chart creates the CNPG
Cluster on `helm install` via a post-install hook, then the web UI takes over
for all subsequent lifecycle operations.

```bash
helm upgrade --install cnpg-ui oci://registry-1.docker.io/clickoniqhub/cnpg-ui \
  --version 0.2.4 \
  --namespace default \
  --wait --timeout 5m \
  --set postgres.create=true \
  --set postgres.appOwner=myapp \
  --set postgres.appDatabase=myapp \
  --set postgres.storageSize=20Gi \
  --set postgres.backup.enabled=true \
  --set postgres.backup.s3Bucket=my-postgres-backups \
  --set postgres.backup.s3Env=prod \
  --set config.s3Bucket=my-postgres-backups \
  --set config.awsRegion=us-east-1 \
  --set ingress.enabled=true \
  --set ingress.tls=true \
  --set ingress.clusterIssuer=letsencrypt \
  --set ingress.host=cnpg-ui.example.com
```

or reuse value 

```
helm upgrade cnpg-ui oci://registry-1.docker.io/clickoniqhub/cnpg-ui --version 0.2.6 --reuse-values
```
After install, the cluster appears under **Settings → Default Cluster** and backup is
wired up automatically. Open the **Backups** page and trigger the first manual backup.

### Option B — Existing cluster (UI only)

Use this when a cluster already exists and you only want to deploy the web interface.

```bash
helm upgrade --install cnpg-ui oci://registry-1.docker.io/clickoniqhub/cnpg-ui \
  --version 0.1.0 \
  --namespace default \
  --set config.defaultCluster=postgres \
  --set config.s3Bucket=my-cnpg-backups \
  --set ingress.enabled=true \
  --set ingress.host=cnpg-ui.example.com
```

Then open the UI and go to **Settings** to fill in the remaining values (S3 env,
restore cluster name, storage sizes, app owner/database).

---

## Configuration Reference

### UI (`config.*`)

| Key | Default | Description |
|---|---|---|
| `config.namespace` | `default` | Kubernetes namespace where CNPG CRDs live |
| `config.awsRegion` | `us-east-1` | AWS region for the S3 bucket |
| `config.s3Bucket` | `""` | S3 bucket name (pre-fills the UI) |
| `config.defaultCluster` | `postgres` | CNPG cluster name the UI targets |
| `config.awsCredentialsSecret` | `aws-credentials` | Secret with `access-key` / `secret-key` |

### PostgreSQL cluster (`postgres.*`)

| Key | Default | Description |
|---|---|---|
| `postgres.create` | `false` | Set `true` to provision a cluster on first install |
| `postgres.clusterName` | `postgres` | Name of the CNPG Cluster CRD |
| `postgres.image` | `ghcr.io/cloudnative-pg/postgresql:17.5` | PostgreSQL container image |
| `postgres.imagePullSecrets` | `[]` | Image pull secrets (e.g. `[regcred]`) |
| `postgres.instances` | `1` | Number of PostgreSQL instances |
| `postgres.appOwner` | `app` | PostgreSQL role written to the `-app` secret |
| `postgres.appDatabase` | `app` | Application database name |
| `postgres.storageSize` | `20Gi` | Data PVC size |
| `postgres.walStorageSize` | `5Gi` | WAL PVC size |
| `postgres.storageClass` | `""` | StorageClass (blank = cluster default) |
| `postgres.nodeSelector` | `{}` | Node selector labels |
| `postgres.tolerations` | `[]` | Pod tolerations |
| `postgres.resources` | see values.yaml | CPU/memory requests and limits |
| `postgres.postgresql.parameters` | `{}` | Raw `postgresql.conf` key/value pairs |
| `postgres.postInitSQL` | `[]` | SQL run as superuser after initdb (e.g. `ALTER ROLE ... CREATEROLE`) |
| `postgres.postInitApplicationSQL` | `[]` | SQL run on the app DB after initdb (e.g. `CREATE EXTENSION`) |

### Backup (`postgres.backup.*`)

| Key | Default | Description |
|---|---|---|
| `postgres.backup.enabled` | `false` | Enable S3 WAL archiving and base backups |
| `postgres.backup.s3Bucket` | `""` | S3 bucket |
| `postgres.backup.s3Env` | `""` | Prefix inside the bucket (e.g. `prod`) |
| `postgres.backup.awsRegion` | `""` | Overrides `config.awsRegion` for backups |
| `postgres.backup.awsCredentialsSecret` | `aws-credentials` | Secret with S3 credentials |
| `postgres.backup.retentionPolicy` | `30d` | CNPG backup retention (e.g. `7d`, `30d`) |
| `postgres.backup.schedule` | `0 0 2 * * 0` | 6-field cron for weekly backups (Sun 02:00 UTC) |

### Ingress (`ingress.*`)

| Key | Default | Description |
|---|---|---|
| `ingress.enabled` | `false` | Create an Ingress resource |
| `ingress.className` | `traefik` | `kubernetes.io/ingress.class` annotation |
| `ingress.host` | `cnpg-ui.example.com` | Hostname |
| `ingress.tls` | `false` | Enable TLS via cert-manager (HTTP-01) |
| `ingress.clusterIssuer` | `letsencrypt` | cert-manager ClusterIssuer name |
| `ingress.annotations` | `{}` | Extra annotations (e.g. IP allowlist middleware) |

---

## Backup & Restore Workflow

### Taking a manual backup

1. Open the **Backups** page.
2. Select the cluster and enter a backup name.
3. Click **Create Backup** and wait for status `Completed`.

The backup includes a `pg_switch_wal()` call immediately after creation to ensure the
final WAL segment is archived to S3 before the backup is considered usable for PITR.

### Point-in-time recovery (PITR)

The full restore flow spans five steps, all triggered from the **Restore** page.

#### Step 1 — Pre-flight check

Enter the target time and click **Check**. The UI verifies:
- A completed base backup exists before the target time.
- WAL coverage on S3 reaches at least the target time.
- The restore cluster's S3 archive is clean (auto-cleaned if dirty).

If the check fails, follow the error messages before proceeding.

#### Step 2 — Start restore cluster

Click **Start Restore**. A temporary cluster named `postgres-restore` (configurable in
Settings) is created from S3. This takes several minutes depending on database size.
Monitor progress on the **Restore** page — wait for phase `Cluster in healthy state`.

#### Step 3 — Verify data

Connect directly to `postgres-restore-rw` to confirm the restored data looks correct
before touching the production cluster.

#### Step 4 — Promote (Restore → Production)

Open the **Promote** page and follow the steps in order:

| Step | Action | What it does |
|---|---|---|
| A | **Hibernate** | Suspends the production `postgres` cluster (stops pods, keeps PVCs) |
| B | **Switch Service** | Points the `postgres` Service selector at `postgres-restore` so the app keeps running |
| C | **Delete Original** | Saves the original cluster's postgresql params and image; detaches PVCs; deletes the Cluster CRD |
| D | **Create Production Cluster** | Creates a new `postgres` cluster from S3 PITR to the same target time as `postgres-restore`; backup config is patched in automatically once the cluster reaches healthy state |
| E | **Switch Service Back** | Points the `postgres` Service back at the new `postgres` cluster |
| F | **Delete Restore Cluster** | Cleans up `postgres-restore` and its PVCs |

> **Why two recoveries?** `postgres-restore` is a verification sandbox. The final
> production cluster is created directly from S3 (not streamed from `postgres-restore`)
> so it has a clean recovery bootstrap and gets the original cluster's postgresql tuning
> restored automatically.

---

## Custom PostgreSQL Parameters

Any `postgresql.conf` parameter can be set under `postgres.postgresql.parameters`.
Values must be strings.

```yaml
postgres:
  postgresql:
    parameters:
      max_connections: "200"
      shared_buffers: "8GB"
      effective_cache_size: "24GB"
      work_mem: "64MB"
      maintenance_work_mem: "512MB"
      checkpoint_completion_target: "0.9"
      wal_buffers: "64MB"
      default_statistics_target: "100"
      random_page_cost: "1.1"        # for SSD storage
      archive_timeout: "5min"        # force WAL archiving at most every 5 minutes
```

Or via `--set` on the command line:

```bash
helm upgrade cnpg-ui cnpg-ui/cnpg-ui \
  --set "postgres.postgresql.parameters.max_connections=300" \
  --set "postgres.postgresql.parameters.shared_buffers=16GB"
```

> Parameters only apply at cluster creation time (`postgres.create: true`).
> To change parameters on a running cluster use the CNPG UI or `kubectl patch`.

---

## Node Affinity & Tolerations

For clusters where the postgres node pool is tainted (e.g. Hetzner k3s with
`workload=postgres:NoSchedule`):

```yaml
postgres:
  nodeSelector:
    workload: postgres
  tolerations:
    - key: workload
      operator: Equal
      value: postgres
      effect: NoSchedule
```

---

## Ingress & TLS

### Traefik with IP allowlist

```yaml
ingress:
  enabled: true
  className: traefik
  host: cnpg-ui.internal.example.com
  tls: false
  annotations:
    traefik.ingress.kubernetes.io/router.middlewares: default-ip-whitelist@kubernetescrd
```

### Traefik with cert-manager TLS

```yaml
ingress:
  enabled: true
  className: traefik
  host: cnpg-ui.example.com
  tls: true
```

cert-manager must be installed and a `letsencrypt` ClusterIssuer must exist.

---

## Upgrading

```bash
helm upgrade cnpg-ui oci://registry-1.docker.io/clickoniqhub/cnpg-ui \
  --version 0.2.1 \
  --wait --timeout 5m \
  -f my-values.yaml
```

---

## Publishing a New Version

```bash
# Fetch the subchart dependency (only needed when building from source)
helm dependency update cnpg-ui/helm

# 1. Bump version in Chart.yaml (e.g. 0.1.9 → 0.2.0)

# 2. Log in to Docker Hub
helm registry login registry-1.docker.io -u clickoniqhub

# 3. Package the chart
helm package cnpg-ui/helm

# 4. Push to Docker Hub OCI registry
helm push cnpg-ui-0.2.0.tgz oci://registry-1.docker.io/clickoniqhub
```

The chart is then available immediately:

```bash
helm install cnpg-ui oci://registry-1.docker.io/clickoniqhub/cnpg-ui --version 0.2.0
```

The `postgres` Cluster CRD is **never modified** by `helm upgrade` — it was created as a
post-install hook and is not part of the release manifest after that.  Changing any
value under `postgres.*` after initial install has no effect on the running cluster.
Use the web UI or `kubectl patch` to change a running cluster.

---

## Uninstalling

```bash
helm uninstall cnpg-ui
```

The PostgreSQL cluster, its PVCs, and its S3 backup archive are **not deleted**
by `helm uninstall` due to the `hook-delete-policy: keep` annotation on the
Cluster CRD. Delete them manually if needed:

```bash
# Delete the cluster (PVCs are detached first to preserve data)
kubectl delete cluster postgres

# Delete PVCs only after confirming data is no longer needed
kubectl delete pvc postgres-1 postgres-1-wal
```
