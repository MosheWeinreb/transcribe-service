"""One async engine and session factory per process; one session per transaction."""

import asyncio

from sqlalchemy import make_url, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.settings.config import settings
from src.transcription.models import Base, Job

engine = create_async_engine(
    make_url(settings.database_url).set(drivername="postgresql+psycopg"),
    pool_size=settings.db_pool_size,
    max_overflow=0,
    pool_timeout=settings.db_timeout_seconds,
    pool_pre_ping=True,
    hide_parameters=True,
    connect_args={
        "connect_timeout": settings.db_timeout_seconds,
        "options": f"-c statement_timeout={settings.db_timeout_seconds * 1000}",
    },
)
sessions = async_sessionmaker(engine, expire_on_commit=False)


async def check_database() -> None:
    async with sessions.begin() as session:
        await session.execute(select(Job.id).limit(0))


async def initialize_schema() -> None:
    # Initial creation only, not a migration of existing tables.
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)


async def main() -> None:
    try:
        await initialize_schema()
        print("Database schema is ready.")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
