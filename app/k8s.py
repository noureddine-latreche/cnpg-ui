import logging
from kubernetes import client, config
from kubernetes.client import CustomObjectsApi, CoreV1Api

logger = logging.getLogger(__name__)

_custom_objects_api: CustomObjectsApi | None = None
_core_v1_api: CoreV1Api | None = None


def get_clients() -> tuple[CustomObjectsApi, CoreV1Api]:
    """
    Returns (CustomObjectsApi, CoreV1Api).
    Tries in-cluster config first, falls back to kubeconfig.
    Singleton — clients are created once and reused.
    """
    global _custom_objects_api, _core_v1_api

    if _custom_objects_api is not None and _core_v1_api is not None:
        return _custom_objects_api, _core_v1_api

    try:
        config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes config")
    except config.ConfigException:
        try:
            config.load_kube_config()
            logger.info("Loaded kubeconfig")
        except config.ConfigException as e:
            logger.error("Failed to load Kubernetes config: %s", e)
            raise

    _custom_objects_api = CustomObjectsApi()
    _core_v1_api = CoreV1Api()
    return _custom_objects_api, _core_v1_api


def reset_clients() -> None:
    """Force re-initialisation of the singleton (useful for testing)."""
    global _custom_objects_api, _core_v1_api
    _custom_objects_api = None
    _core_v1_api = None
