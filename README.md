# CNPG UI

A local web interface for [CloudNativePG](https://cloudnative-pg.io/) that covers the full PostgreSQL lifecycle: cluster status, scheduled backups, and point-in-time recovery (PITR).

Runs on your laptop (or any machine with kubeconfig access) and connects to your cluster over the Kubernetes API — no in-cluster deployment required. Switch between clusters using the context switcher in the sidebar.

---

## Quick Start — Docker

**1. Run with your kubeconfig mounted:**

```bash
docker run -p 8080:8080 \
  -v ~/.kube:/root/.kube:ro \
  -v cnpg-data:/data \
  -e AWS_REGION=eu-west-2 \
  -e S3_BUCKET=my-postgres-backups \
  clickoniqhub/cnpg-ui:latest
```

Open [http://localhost:8080](http://localhost:8080). The context switcher in the sidebar lists every context in your kubeconfig — select the cluster you want to manage and the UI reconnects immediately.

**2. Docker Compose (recommended for repeated use):**

```yaml
services:
  cnpg-ui:
    image: clickoniqhub/cnpg-ui:latest
    volumes:
      - ~/.kube:/root/.kube:ro   # or a single file: ./kubeconfig.yaml:/root/.kube/config:ro
      - cnpg-data:/data
    environment:
      AWS_REGION: eu-west-2
      S3_BUCKET: my-postgres-backups
    ports:
      - "8080:8080"

volumes:
  cnpg-data:
```

```bash
docker compose up
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-1` | AWS region for the S3 backup bucket |
| `S3_BUCKET` | `""` | S3 bucket name (pre-fills the UI) |
| `DEFAULT_CLUSTER` | `postgres` | CNPG cluster name targeted by default |
| `NAMESPACE` | `default` | Kubernetes namespace where CNPG CRDs live |
| `AWS_CREDENTIALS_SECRET` | `aws-credentials` | Kubernetes Secret name with `access-key` / `secret-key` |
| `STORAGE_CLASS` | `""` | StorageClass for restore PVCs (blank = cluster default) |
| `NODE_SELECTOR` | `{}` | JSON-encoded node selector for programmatic cluster creation |
| `TOLERATIONS` | `[]` | JSON-encoded tolerations for programmatic cluster creation |
| `DB_PATH` | `/data/cnpg-ui.db` | SQLite database path (persisted via volume) |

All of these can also be configured after startup via **Settings** in the UI.

**Node affinity example (Hetzner k3s with tainted node pool):**

```bash
-e STORAGE_CLASS=hcloud-volumes
-e NODE_SELECTOR='{"workload":"postgres"}'
-e TOLERATIONS='[{"key":"workload","operator":"Equal","value":"postgres","effect":"NoSchedule"}]'
```

---

## AWS Credentials

The UI needs AWS credentials to scan S3 for WAL archives and base backups. Two options:

**Option A — Kubernetes Secret (recommended when the cluster has an `aws-credentials` secret):**

The UI reads `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from environment variables. Inject them from the secret your cluster already uses:

```bash
docker run ... \
  -e AWS_ACCESS_KEY_ID=$(kubectl get secret aws-credentials -o jsonpath='{.data.access-key}' | base64 -d) \
  -e AWS_SECRET_ACCESS_KEY=$(kubectl get secret aws-credentials -o jsonpath='{.data.secret-key}' | base64 -d) \
  clickoniqhub/cnpg-ui:latest
```

**Option B — Local AWS profile:**

```bash
docker run ... \
  -v ~/.aws:/root/.aws:ro \
  clickoniqhub/cnpg-ui:latest
```

---

## Switching Between Clusters

Every context defined in your kubeconfig appears in the dropdown at the bottom of the sidebar. Selecting one:

1. Switches the Kubernetes client to that context
2. Reloads the page — the dashboard now shows clusters, backups, and health for the selected cluster

The **Namespace** setting (in Settings) is per-session. After switching contexts you may need to update it if your CNPG clusters live in a different namespace on the new cluster.

---

## Backup & Restore Workflow

### Taking a manual backup

1. Open the **Backups** page.
2. Select the cluster and enter a backup name.
3. Click **Create Backup** and wait for status `Completed`.

A `pg_switch_wal()` is issued immediately after creation to flush the final WAL segment to S3 before the backup is considered usable for PITR.

### Point-in-time recovery (PITR)

The full restore flow spans five steps, all triggered from the **Restore** page.

#### Step 1 — Pre-flight check

Enter the target time and click **Check**. The UI verifies:
- A completed base backup exists before the target time.
- WAL coverage on S3 reaches at least the target time.
- The restore cluster's S3 archive is clean (auto-cleaned if dirty).

#### Step 2 — Start restore cluster

Click **Start Restore**. A temporary cluster named `postgres-restore` (configurable in Settings) is created from S3. Monitor progress on the Restore page — wait for phase `Cluster in healthy state`.

#### Step 3 — Verify data

Connect directly to `postgres-restore-rw` to confirm the restored data looks correct before touching the production cluster.

#### Step 4 — Promote (Restore → Production)

Open the **Promote** section and follow the steps in order:

| Step | Action | What it does |
|---|---|---|
| A | **Hibernate** | Suspends the production cluster (stops pods, keeps PVCs) |
| B | **Switch Service** | Points the `postgres` Service selector at `postgres-restore` |
| C | **Delete Original** | Saves postgresql params and image; detaches PVCs; deletes the Cluster CRD |
| D | **Create Production Cluster** | Creates a new `postgres` cluster from S3 PITR to the same target time; backup config is patched in automatically once healthy |
| E | **Switch Service Back** | Points the `postgres` Service back at the new cluster |
| F | **Delete Restore Cluster** | Cleans up `postgres-restore` and its PVCs |

> **Why two recoveries?** `postgres-restore` is a verification sandbox. The final
> production cluster is created directly from S3 (not streamed from `postgres-restore`)
> so it has a clean recovery bootstrap and retains the original cluster's tuning profile.

---

## Helm (in-cluster deployment)

If you want a shared team instance deployed into the cluster rather than running locally, the Helm chart is available:

```bash
helm upgrade --install cnpg-ui oci://registry-1.docker.io/clickoniqhub/cnpg-ui \
  --version 0.2.6 \
  --namespace default \
  --set config.s3Bucket=my-postgres-backups \
  --set config.awsRegion=eu-west-2 \
  --set ingress.enabled=true \
  --set ingress.host=cnpg-ui.internal.example.com
```

> Context switching is not available in in-cluster mode — the UI connects to the cluster it runs in.

### Key Helm values

| Key | Default | Description |
|---|---|---|
| `config.namespace` | `default` | Namespace where CNPG CRDs live |
| `config.awsRegion` | `us-east-1` | AWS region |
| `config.s3Bucket` | `""` | S3 bucket |
| `config.defaultCluster` | `postgres` | Default cluster name |
| `postgres.create` | `false` | Provision a new CNPG Cluster on install |
| `postgres.storageSize` | `20Gi` | Data PVC size |
| `postgres.storageClass` | `""` | StorageClass (blank = cluster default) |
| `postgres.nodeSelector` | `{}` | Node selector for postgres pods |
| `postgres.tolerations` | `[]` | Tolerations for postgres pods |
| `postgres.backup.enabled` | `false` | Enable S3 WAL archiving |
| `postgres.backup.s3Bucket` | `""` | Backup S3 bucket |
| `postgres.backup.schedule` | `0 0 2 * * 0` | 6-field cron (default: Sunday 02:00 UTC) |
| `ingress.enabled` | `false` | Create an Ingress resource |
| `ingress.host` | `cnpg-ui.example.com` | Hostname |
| `ingress.tls` | `false` | TLS via cert-manager |

---

## Publishing a New Version

```bash
helm dependency update cnpg-ui/helm
# Bump version in Chart.yaml
helm registry login registry-1.docker.io -u clickoniqhub
helm package cnpg-ui/helm
helm push cnpg-ui-<version>.tgz oci://registry-1.docker.io/clickoniqhub
```
