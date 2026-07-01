import base64
import logging
from typing import Any

from kubernetes.client.exceptions import ApiException
from kubernetes.stream import stream

from .k8s import get_clients
from .config import settings

logger = logging.getLogger(__name__)

GROUP = "postgresql.cnpg.io"
VERSION = "v1"
CLUSTERS_PLURAL = "clusters"
BACKUPS_PLURAL = "backups"
SCHEDULED_BACKUPS_PLURAL = "scheduledbackups"

RESTORE_CLUSTER_NAME = "postgres-restore"


class CNPGClient:
    def __init__(self, namespace: str | None = None):
        self.namespace = namespace or settings.NAMESPACE
        self._custom, self._core = get_clients()

    # ------------------------------------------------------------------
    # Clusters
    # ------------------------------------------------------------------

    def list_clusters(self) -> list[dict]:
        try:
            result = self._custom.list_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=CLUSTERS_PLURAL,
            )
            return result.get("items", [])
        except ApiException as e:
            if e.status == 404:
                return []
            logger.error("Error listing clusters: %s", e)
            raise

    def get_cluster(self, name: str) -> dict | None:
        try:
            return self._custom.get_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=CLUSTERS_PLURAL,
                name=name,
            )
        except ApiException as e:
            if e.status == 404:
                return None
            logger.error("Error getting cluster %s: %s", name, e)
            raise

    # ------------------------------------------------------------------
    # Backups
    # ------------------------------------------------------------------

    def list_backups(self, cluster_name: str | None = None) -> list[dict]:
        try:
            result = self._custom.list_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=BACKUPS_PLURAL,
            )
            items = result.get("items", [])
            if cluster_name:
                items = [
                    b for b in items
                    if b.get("spec", {}).get("cluster", {}).get("name") == cluster_name
                ]
            return items
        except ApiException as e:
            if e.status == 404:
                return []
            logger.error("Error listing backups: %s", e)
            raise

    def get_backup(self, name: str) -> dict | None:
        try:
            return self._custom.get_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=BACKUPS_PLURAL,
                name=name,
            )
        except ApiException as e:
            if e.status == 404:
                return None
            logger.error("Error getting backup %s: %s", name, e)
            raise

    def create_backup(self, cluster_name: str, backup_name: str) -> dict:
        body = {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "Backup",
            "metadata": {
                "name": backup_name,
                "namespace": self.namespace,
            },
            "spec": {
                "cluster": {"name": cluster_name},
                "target": "primary",
                "method": "barmanObjectStore",
            },
        }
        try:
            return self._custom.create_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=BACKUPS_PLURAL,
                body=body,
            )
        except ApiException as e:
            logger.error("Error creating backup %s: %s", backup_name, e)
            raise

    def delete_backup(self, name: str) -> None:
        try:
            self._custom.delete_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=BACKUPS_PLURAL,
                name=name,
            )
        except ApiException as e:
            if e.status == 404:
                return
            logger.error("Error deleting backup %s: %s", name, e)
            raise

    # ------------------------------------------------------------------
    # Restore cluster
    # ------------------------------------------------------------------

    def get_restore_cluster(self, restore_name: str = RESTORE_CLUSTER_NAME) -> dict | None:
        return self.get_cluster(restore_name)

    @staticmethod
    def _s3_credentials(aws_credentials_secret: str) -> dict:
        return {
            "accessKeyId": {"name": aws_credentials_secret, "key": "access-key"},
            "secretAccessKey": {"name": aws_credentials_secret, "key": "secret-key"},
        }

    @staticmethod
    def _cluster_infra(
        storage_size: str = "100Gi",
        wal_storage_size: str = "20Gi",
        storage_class: str = "",
        node_selector: dict | None = None,
        tolerations: list | None = None,
    ) -> dict:
        """Base infrastructure spec for temporary clusters.

        No backup section — restore clusters are ephemeral and must not archive
        WALs to S3. CNPG rejects cluster startup with 'Expected empty archive'
        if the destination already contains data from a previous restore.
        """
        storage: dict = {"size": storage_size}
        if storage_class:
            storage["storageClass"] = storage_class
        wal_storage: dict = {"size": wal_storage_size}
        if storage_class:
            wal_storage["storageClass"] = storage_class

        infra: dict = {
            "instances": 1,
            "imageName": "ghcr.io/cloudnative-pg/postgresql:17.5",
            "imagePullPolicy": "IfNotPresent",
            "env": [
                {"name": "AWS_DEFAULT_REGION", "value": settings.AWS_REGION},
                {"name": "AWS_EC2_METADATA_DISABLED", "value": "true"},
            ],
            "storage": storage,
            "walStorage": wal_storage,
            "resources": {
                "requests": {"memory": "2Gi", "cpu": "1000m"},
                "limits": {"cpu": "4000m", "memory": "12Gi"},
            },
        }

        if node_selector or tolerations:
            affinity: dict = {}
            if node_selector:
                affinity["nodeSelector"] = node_selector
            if tolerations:
                affinity["tolerations"] = tolerations
            infra["affinity"] = affinity

        return infra

    def create_restore_cluster(
        self,
        target_time: str,
        source_cluster: str,
        destination_path: str,
        aws_credentials_secret: str,
        restore_name: str = RESTORE_CLUSTER_NAME,
        storage_size: str = "100Gi",
        wal_storage_size: str = "20Gi",
        storage_class: str = "",
        node_selector: dict | None = None,
        tolerations: list | None = None,
    ) -> dict:
        body = {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "Cluster",
            "metadata": {"name": restore_name, "namespace": self.namespace},
            "spec": {
                **self._cluster_infra(storage_size, wal_storage_size, storage_class, node_selector, tolerations),
                "bootstrap": {
                    "recovery": {
                        "source": "cnpg-backup-source",
                        "recoveryTarget": {"targetTime": target_time},
                    }
                },
                "externalClusters": [
                    {
                        "name": "cnpg-backup-source",
                        "barmanObjectStore": {
                            "destinationPath": destination_path,
                            "serverName": source_cluster,
                            "s3Credentials": self._s3_credentials(aws_credentials_secret),
                            "wal": {"compression": "gzip", "maxParallel": 8},
                        },
                    }
                ],
            },
        }
        try:
            return self._custom.create_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=CLUSTERS_PLURAL,
                body=body,
            )
        except ApiException as e:
            logger.error("Error creating restore cluster: %s", e)
            raise

    def create_production_cluster(
        self,
        cluster_name: str,
        target_time: str,
        source_cluster_s3_name: str,
        destination_path: str,
        aws_credentials_secret: str,
        storage_size: str = "100Gi",
        wal_storage_size: str = "20Gi",
        storage_class: str = "",
        node_selector: dict | None = None,
        tolerations: list | None = None,
        app_owner: str = "app",
        app_database: str = "app",
        saved_postgresql_spec: dict | None = None,
        saved_resources_spec: dict | None = None,
        image_name: str | None = None,
    ) -> dict:
        """Create the production-named cluster from S3 PITR — no pg_basebackup streaming.

        Backup config is intentionally omitted here: CNPG's empty-archive check would
        reject startup because the original cluster's WALs still live at that S3 path.
        A background task patches in backup config once the cluster reaches a healthy state.
        """
        infra = self._cluster_infra(storage_size, wal_storage_size, storage_class, node_selector, tolerations)
        if image_name:
            infra["imageName"] = image_name
        if saved_resources_spec:
            infra["resources"] = saved_resources_spec

        spec: dict = {
            **infra,
            "bootstrap": {
                "recovery": {
                    "source": "cnpg-backup-source",
                    "recoveryTarget": {"targetTime": target_time},
                    "owner": app_owner,
                    "database": app_database,
                }
            },
            "externalClusters": [
                {
                    "name": "cnpg-backup-source",
                    "barmanObjectStore": {
                        "destinationPath": destination_path,
                        "serverName": source_cluster_s3_name,
                        "s3Credentials": self._s3_credentials(aws_credentials_secret),
                        "wal": {"compression": "gzip", "maxParallel": 8},
                    },
                }
            ],
        }
        if saved_postgresql_spec:
            spec["postgresql"] = saved_postgresql_spec

        body = {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "Cluster",
            "metadata": {"name": cluster_name, "namespace": self.namespace},
            "spec": spec,
        }
        try:
            return self._custom.create_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=CLUSTERS_PLURAL,
                body=body,
            )
        except ApiException as e:
            logger.error("Error creating production cluster %s: %s", cluster_name, e)
            raise

    def create_finalize_cluster(
        self,
        final_name: str,
        restore_name: str,
        destination_path: str | None = None,
        aws_credentials_secret: str | None = None,
    ) -> dict:
        """Create a new cluster named final_name by streaming a base backup from restore_name.

        Uses CNPG's pg_basebackup bootstrap: streams the full data directory from the
        live restore cluster over the network, then replays WALs to catch up. No S3
        backup required — the source cluster just needs to be running.

        Storage sizes are copied from the source cluster so this works regardless of
        database size.

        When destination_path and aws_credentials_secret are provided the new cluster is
        created with a full backup/WAL-archiving spec so it is immediately consistent
        with what the Helm chart expects.  Without them the function falls back to
        copying the backup spec from the source (if any).
        """
        source = self.get_cluster(restore_name)
        if not source:
            raise ValueError(f"Source cluster '{restore_name}' not found")
        source_spec = source.get("spec", {})
        storage = source_spec.get("storage", {"storageClass": "hcloud-volumes", "size": "100Gi"})
        wal_storage = source_spec.get("walStorage", {"storageClass": "hcloud-volumes", "size": "20Gi"})

        # Prefer explicit settings so the cluster always matches what the Helm chart
        # deploys, even when the source cluster (postgres-restore) has no backup config.
        backup_section: dict = {}
        if destination_path and aws_credentials_secret:
            backup_section = {
                "backup": {
                    "barmanObjectStore": {
                        "destinationPath": destination_path,
                        "s3Credentials": self._s3_credentials(aws_credentials_secret),
                        "wal": {"compression": "gzip", "maxParallel": 8},
                        "data": {"compression": "gzip", "immediateCheckpoint": False, "jobs": 8},
                    },
                    "retentionPolicy": "30d",
                }
            }
        else:
            # Fall back to copying from the source cluster if it has backup configured.
            # Strip any explicit serverName so the new cluster archives under its own name.
            source_bos = source_spec.get("backup", {}).get("barmanObjectStore", {})
            if source_bos:
                bos = {k: v for k, v in source_bos.items() if k != "serverName"}
                backup_section = {"backup": {"barmanObjectStore": bos}}

        # Determine the app database/owner from the pre-existing {final_name}-app secret
        # (created by Helm/ESO, survives cluster deletion). Falls back to CNPG default
        # "app" only if the secret is absent or unreadable.
        app_owner = "app"
        app_database = "app"
        try:
            secret = self._core.read_namespaced_secret(
                name=f"{final_name}-app", namespace=self.namespace
            )
            if secret.data:
                raw_user = secret.data.get("username") or secret.data.get("user")
                raw_db = secret.data.get("dbname") or secret.data.get("database")
                if raw_user:
                    app_owner = base64.b64decode(raw_user).decode()
                if raw_db:
                    app_database = base64.b64decode(raw_db).decode()
        except ApiException as e:
            if e.status != 404:
                logger.warning("Could not read %s-app secret: %s", final_name, e)

        env = list(source_spec.get("env", [{"name": "AWS_EC2_METADATA_DISABLED", "value": "true"}]))
        # Ensure AWS_DEFAULT_REGION is present if the source had it
        env_names = {e["name"] for e in env}
        if "AWS_DEFAULT_REGION" not in env_names:
            env.append({"name": "AWS_DEFAULT_REGION", "value": settings.AWS_REGION})

        body = {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "Cluster",
            "metadata": {"name": final_name, "namespace": self.namespace},
            "spec": {
                "instances": 1,
                "imageName": source_spec.get("imageName", "ghcr.io/cloudnative-pg/postgresql:17.5"),
                "imagePullPolicy": "IfNotPresent",
                **backup_section,
                "bootstrap": {
                    "pg_basebackup": {
                        "source": restore_name,
                        "database": app_database,
                        "owner": app_owner,
                    },
                },
                "externalClusters": [
                    {
                        "name": restore_name,
                        "connectionParameters": {
                            "host": f"{restore_name}-rw",
                            "user": "streaming_replica",
                            "sslmode": "verify-ca",
                            "dbname": "postgres",
                        },
                        "sslCert": {
                            "name": f"{restore_name}-replication",
                            "key": "tls.crt",
                        },
                        "sslKey": {
                            "name": f"{restore_name}-replication",
                            "key": "tls.key",
                        },
                        "sslRootCert": {
                            "name": f"{restore_name}-ca",
                            "key": "ca.crt",
                        },
                    }
                ],
                "env": env,
                "storage": storage,
                "walStorage": wal_storage,
                "resources": source_spec.get("resources", {
                    "requests": {"memory": "2Gi", "cpu": "1000m"},
                    "limits": {"cpu": "4000m", "memory": "12Gi"},
                }),
                "affinity": source_spec.get("affinity", {
                    "nodeSelector": {"workload": "postgres"},
                    "tolerations": [
                        {"key": "workload", "operator": "Equal", "value": "postgres", "effect": "NoSchedule"}
                    ],
                }),
            },
        }
        try:
            return self._custom.create_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=CLUSTERS_PLURAL,
                body=body,
            )
        except ApiException as e:
            logger.error("Error creating finalize cluster: %s", e)
            raise

    def switch_wal(self, cluster_name: str) -> str:
        """Force the primary to archive the current WAL segment via pg_switch_wal().

        A base backup's end WAL only archives when the segment fills (16MB) or
        archive_timeout fires. Calling this immediately after a backup ensures the
        end WAL is in S3 before a restore is attempted.
        Returns the new WAL position string.
        """
        primary = self.get_cluster(cluster_name)
        if not primary:
            raise ValueError(f"Cluster '{cluster_name}' not found")
        primary_pod = primary.get("status", {}).get("currentPrimary")
        if not primary_pod:
            raise ValueError(f"No primary pod found for cluster '{cluster_name}'")
        resp = stream(
            self._core.connect_get_namespaced_pod_exec,
            name=primary_pod,
            namespace=self.namespace,
            container="postgres",
            command=["psql", "-U", "postgres", "-tAc", "SELECT pg_switch_wal()"],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
        return resp.strip()

    def list_cluster_pvcs(self, cluster_name: str) -> list[str]:
        """Return names of all PVCs labelled cnpg.io/cluster=<cluster_name>."""
        result = self._core.list_namespaced_persistent_volume_claim(
            namespace=self.namespace,
            label_selector=f"cnpg.io/cluster={cluster_name}",
        )
        return [pvc.metadata.name for pvc in result.items]

    def detach_cluster_pvcs(self, cluster_name: str) -> list[str]:
        """Strip ownerReferences from cluster PVCs so they survive CRD deletion.

        Uses application/merge-patch+json where null unambiguously means "remove the
        field". Strategic merge patch (the default) has undefined behaviour for null on
        list fields across Kubernetes versions and may silently leave the field intact,
        causing Kubernetes GC to delete the PVCs when the Cluster CRD is deleted.
        """
        names = self.list_cluster_pvcs(cluster_name)
        detached = []
        config = self._core.api_client.configuration
        auth_value = (config.api_key or {}).get("authorization", "")
        auth_prefix = (config.api_key_prefix or {}).get("authorization", "")
        auth_header = f"{auth_prefix} {auth_value}".strip() if auth_prefix else auth_value
        for name in names:
            url = f"{config.host}/api/v1/namespaces/{self.namespace}/persistentvolumeclaims/{name}"
            try:
                resp = self._core.api_client.rest_client.PATCH(
                    url,
                    headers={
                        "Content-Type": "application/merge-patch+json",
                        "Accept": "application/json",
                        "Authorization": auth_header,
                    },
                    body={"metadata": {"ownerReferences": None}},
                )
                if resp.status >= 400:
                    logger.warning("Could not detach PVC %s: %s %s", name, resp.status, resp.data)
                else:
                    detached.append(name)
            except Exception as e:
                logger.warning("Could not detach PVC %s: %s", name, e)
        return detached

    def delete_named_pvcs(self, pvc_names: list[str]) -> None:
        for name in pvc_names:
            try:
                self._core.delete_namespaced_persistent_volume_claim(
                    name=name, namespace=self.namespace,
                )
            except ApiException as e:
                if e.status != 404:
                    logger.error("Error deleting PVC %s: %s", name, e)
                    raise

    def delete_restore_cluster(self, restore_name: str = RESTORE_CLUSTER_NAME) -> None:
        try:
            self._custom.delete_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=CLUSTERS_PLURAL,
                name=restore_name,
            )
        except ApiException as e:
            if e.status != 404:
                logger.error("Error deleting restore cluster: %s", e)
                raise

        for pvc_name in [f"{restore_name}-1", f"{restore_name}-1-wal"]:
            try:
                self._core.delete_namespaced_persistent_volume_claim(
                    name=pvc_name, namespace=self.namespace,
                )
            except ApiException as e:
                if e.status != 404:
                    logger.error("Error deleting PVC %s: %s", pvc_name, e)
                    raise

    # ------------------------------------------------------------------
    # Hibernation / promote
    # ------------------------------------------------------------------

    def _merge_patch_crd(self, plural: str, name: str, body: dict) -> None:
        """Generic merge-patch for any CRD in the CNPG API group."""
        config = self._custom.api_client.configuration
        url = (
            f"{config.host}/apis/{GROUP}/{VERSION}"
            f"/namespaces/{self.namespace}/{plural}/{name}"
        )
        auth_value = (config.api_key or {}).get("authorization", "")
        auth_prefix = (config.api_key_prefix or {}).get("authorization", "")
        auth_header = f"{auth_prefix} {auth_value}".strip() if auth_prefix else auth_value

        resp = self._custom.api_client.rest_client.PATCH(
            url,
            headers={
                "Content-Type": "application/merge-patch+json",
                "Accept": "application/json",
                "Authorization": auth_header,
            },
            body=body,
        )
        if resp.status >= 400:
            logger.error("merge-patch %s/%s → %s: %s", plural, name, resp.status, resp.data)
            raise ApiException(status=resp.status, reason=resp.data)

    def update_backup_retention(self, cluster_name: str, retention: str) -> None:
        """Patch the cluster's barman retention policy."""
        self._merge_patch_cluster(
            cluster_name,
            {"spec": {"backup": {"retentionPolicy": retention}}},
        )

    def _merge_patch_cluster(self, name: str, body: dict) -> None:
        self._merge_patch_crd(CLUSTERS_PLURAL, name, body)

    def hibernate_cluster(self, name: str) -> None:
        self._merge_patch_cluster(
            name, {"metadata": {"annotations": {"cnpg.io/hibernation": "on"}}}
        )

    def unhibernate_cluster(self, name: str) -> None:
        self._merge_patch_cluster(
            name, {"metadata": {"annotations": {"cnpg.io/hibernation": "off"}}}
        )

    def get_service_info(self, name: str) -> dict | None:
        try:
            svc = self._core.read_namespaced_service(name=name, namespace=self.namespace)
            selector = (svc.spec.selector or {}) if svc.spec else {}
            return {
                "name": name,
                "cluster_selector": selector.get("cnpg.io/cluster"),
                "selector": selector,
            }
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def patch_service_cluster_selector(self, service_name: str, cluster_name: str) -> None:
        self._core.patch_namespaced_service(
            name=service_name,
            namespace=self.namespace,
            body={"spec": {"selector": {"cnpg.io/cluster": cluster_name}}},
        )

    def delete_cluster(self, name: str) -> None:
        try:
            self._custom.delete_namespaced_custom_object(
                group=GROUP,
                version=VERSION,
                namespace=self.namespace,
                plural=CLUSTERS_PLURAL,
                name=name,
            )
        except ApiException as e:
            if e.status != 404:
                logger.error("Error deleting cluster %s: %s", name, e)
                raise

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    @staticmethod
    def is_hibernated(cluster: dict) -> bool:
        annotations = (cluster.get("metadata") or {}).get("annotations") or {}
        return annotations.get("cnpg.io/hibernation") == "on"

    @staticmethod
    def cluster_phase(cluster: dict) -> str:
        return cluster.get("status", {}).get("phase", "Unknown")

    @staticmethod
    def cluster_conditions(cluster: dict) -> list[dict]:
        return cluster.get("status", {}).get("conditions", [])

    @staticmethod
    def is_archiving_healthy(cluster: dict) -> bool:
        conditions = cluster.get("status", {}).get("conditions", [])
        for cond in conditions:
            if cond.get("type") == "ContinuousArchiving":
                return cond.get("status") == "True"
        return False

    @staticmethod
    def cluster_summary(cluster: dict) -> dict[str, Any]:
        """Return a flat dict with the most useful cluster fields."""
        status = cluster.get("status", {})
        meta = cluster.get("metadata", {})
        spec = cluster.get("spec", {})
        return {
            "name": meta.get("name", ""),
            "namespace": meta.get("namespace", ""),
            "phase": status.get("phase", "Unknown"),
            "primary": status.get("currentPrimary", ""),
            "ready_instances": status.get("readyInstances", 0),
            "instances": spec.get("instances", 1),
            "image": spec.get("imageName", ""),
            "archiving_healthy": CNPGClient.is_archiving_healthy(cluster),
            "conditions": CNPGClient.cluster_conditions(cluster),
            "created_at": meta.get("creationTimestamp", ""),
            "hibernated": CNPGClient.is_hibernated(cluster),
        }
