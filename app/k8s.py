import logging
from kubernetes import client, config
from kubernetes.client import CustomObjectsApi, CoreV1Api

logger = logging.getLogger(__name__)

_custom_objects_api: CustomObjectsApi | None = None
_core_v1_api: CoreV1Api | None = None
_active_context: str | None = None


def get_clients() -> tuple[CustomObjectsApi, CoreV1Api]:
    """
    Returns (CustomObjectsApi, CoreV1Api).
    Tries in-cluster config first, falls back to kubeconfig.
    Singleton — clients are recreated when set_context() is called.
    """
    global _custom_objects_api, _core_v1_api

    if _custom_objects_api is not None and _core_v1_api is not None:
        return _custom_objects_api, _core_v1_api

    try:
        config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes config")
    except config.ConfigException:
        try:
            config.load_kube_config(context=_active_context)
            logger.info("Loaded kubeconfig (context: %s)", _active_context or "default")
        except config.ConfigException as e:
            logger.error("Failed to load Kubernetes config: %s", e)
            raise

    _custom_objects_api = CustomObjectsApi()
    _core_v1_api = CoreV1Api()
    return _custom_objects_api, _core_v1_api


def reset_clients() -> None:
    """Force re-initialisation of the singleton (useful for testing and context switches)."""
    global _custom_objects_api, _core_v1_api
    _custom_objects_api = None
    _core_v1_api = None


def list_contexts() -> list[str]:
    """Return all context names from kubeconfig, or empty list if unavailable."""
    try:
        contexts, _ = config.list_kube_config_contexts()
        return [c["name"] for c in (contexts or [])]
    except Exception:
        return []


def get_current_context() -> str:
    """Return the active context name (explicit override, then kubeconfig default)."""
    if _active_context:
        return _active_context
    try:
        _, active = config.list_kube_config_contexts()
        return active["name"] if active else ""
    except Exception:
        return ""


def set_context(context_name: str) -> None:
    """Switch to a different kubeconfig context and discard the cached clients."""
    global _active_context
    _active_context = context_name
    reset_clients()
