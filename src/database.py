from functools import lru_cache

from sqlalchemy import Engine, create_engine, make_url, select
from sqlalchemy.orm import Session, sessionmaker

from src.settings.config import get_settings
from src.transcription.models import Base, Job


@lru_cache
def get_engine() -> Engine:
    settings = get_settings()
    url = make_url(settings.database_url).set(drivername="postgresql+psycopg")
    return create_engine(
        url,
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


@lru_cache(maxsize=1)
def session_factory() -> sessionmaker[Session]:
    # Reuse the factory; each begin() creates an independent session and transaction.
    return sessionmaker(get_engine(), expire_on_commit=False)


def close_database() -> None:
    session_factory.cache_clear()
    if get_engine.cache_info().currsize:
        get_engine().dispose()
        get_engine.cache_clear()


def check_database() -> None:
    with session_factory().begin() as session:
        session.execute(select(Job.id).limit(0))


def initialize_schema() -> None:
    # Initial creation only. Schema changes still need versioned migrations.
    Base.metadata.create_all(get_engine())


if __name__ == "__main__":
    try:
        initialize_schema()
        print("Database schema is ready.")
    finally:
        close_database()
