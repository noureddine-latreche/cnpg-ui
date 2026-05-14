import logging
from typing import Any

from sqlalchemy import Column, Integer, String, select, delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from .config import settings

logger = logging.getLogger(__name__)

_engine = None
_session_factory = None

DEFAULT_SETTINGS = {
    "namespace": "default",
    "aws_region": "us-east-1",
    "s3_bucket": "",
    "s3_env": "",
    "default_cluster": "postgres",
    "restore_cluster_name": "postgres-restore",
    "aws_credentials_secret": "aws-credentials",
    "storage_size": "100Gi",
    "wal_storage_size": "20Gi",
    "app_owner": "app",
    "app_database": "app",
    "backup_schedule": "0 0 2 * * 0",
    "backup_retention": "30d",
}


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
