import logging
from typing import Any

from sqlalchemy import Column, Integer, String, select, delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from .config import settings

logger = logging.getLogger(__name__)

_engine = None
_session_factory = None

# Settings that are scoped to a specific CNPG cluster.
CLUSTER_SETTINGS_KEYS: frozenset[str] = frozenset({
    "s3_bucket",
    "s3_env",
    "storage_class",
    "storage_size",
    "wal_storage_size",
    "app_owner",
    "app_database",
    "restore_cluster_name",
    "aws_credentials_secret",
})

# Settings that apply globally (not per-cluster).
GLOBAL_SETTINGS_KEYS: frozenset[str] = frozenset({
    "namespace",
    "aws_region",
    "default_cluster",
})

DEFAULT_GLOBAL_SETTINGS = {
    "namespace": "default",
    "aws_region": "us-east-1",
    "default_cluster": "postgres",
}

DEFAULT_CLUSTER_SETTINGS = {
    "s3_bucket": "",
    "s3_env": "",
    "restore_cluster_name": "postgres-restore",
    "aws_credentials_secret": "aws-credentials",
    "storage_size": "100Gi",
    "wal_storage_size": "20Gi",
    "storage_class": "",
    "app_owner": "app",
    "app_database": "app",
}

# Keep for backwards compatibility with existing DBs that have flat keys.
DEFAULT_SETTINGS = {**DEFAULT_GLOBAL_SETTINGS, **DEFAULT_CLUSTER_SETTINGS}


def _cluster_key(context: str, cluster_name: str, key: str) -> str:
    return f"cluster:{context}:{cluster_name}:{key}"


class Base(DeclarativeBase):
    pass


class Settings(Base):
    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, unique=True, nullable=False, index=True)
    value = Column(String, nullable=False, default="")


def _get_db_url() -> str:
    return f"sqlite+aiosqlite:///{settings.DB_PATH}"


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_async_engine(_get_db_url(), echo=False)
    return _engine


def get_session_factory():
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=get_engine(), class_=AsyncSession, expire_on_commit=False
        )
    return _session_factory


async def init_db() -> None:
    """Create tables and seed default settings."""
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Seed defaults — only insert keys that don't already exist
    async with get_session_factory()() as session:
        for key, value in DEFAULT_SETTINGS.items():
            result = await session.execute(select(Settings).where(Settings.key == key))
            existing = result.scalar_one_or_none()
            if existing is None:
                session.add(Settings(key=key, value=value))
        await session.commit()

    logger.info("Database initialised at %s", settings.DB_PATH)


async def get_setting(key: str, default: str | None = None) -> str | None:
    async with get_session_factory()() as session:
        result = await session.execute(select(Settings).where(Settings.key == key))
        row = result.scalar_one_or_none()
        if row is None:
            return default
        return row.value


async def set_setting(key: str, value: str) -> None:
    async with get_session_factory()() as session:
        result = await session.execute(select(Settings).where(Settings.key == key))
        row = result.scalar_one_or_none()
        if row is None:
            session.add(Settings(key=key, value=value))
        else:
            row.value = value
        await session.commit()


async def get_all_settings() -> dict[str, Any]:
    async with get_session_factory()() as session:
        result = await session.execute(select(Settings))
        rows = result.scalars().all()
        return {row.key: row.value for row in rows}


async def get_cluster_settings(context: str, cluster_name: str) -> dict[str, str]:
    """Return per-cluster settings for (context, cluster_name), keys without the prefix."""
    prefix = f"cluster:{context}:{cluster_name}:"
    async with get_session_factory()() as session:
        result = await session.execute(select(Settings))
        rows = result.scalars().all()
        return {
            row.key[len(prefix):]: row.value
            for row in rows
            if row.key.startswith(prefix)
        }


async def set_cluster_setting(context: str, cluster_name: str, key: str, value: str) -> None:
    await set_setting(_cluster_key(context, cluster_name, key), value)


async def get_effective_settings(context: str, cluster_name: str) -> dict[str, Any]:
    """Merge global settings with (context, cluster)-specific overrides.

    Priority: defaults → global flat keys (legacy) → per-(context, cluster) settings.
    """
    all_rows = await get_all_settings()

    global_settings = {
        k: v for k, v in all_rows.items()
        if not k.startswith("cluster:") and not k.startswith("_")
    }

    cluster_settings = await get_cluster_settings(context, cluster_name)

    result = {**DEFAULT_CLUSTER_SETTINGS, **DEFAULT_GLOBAL_SETTINGS}
    result.update(global_settings)
    result.update(cluster_settings)
    return result
