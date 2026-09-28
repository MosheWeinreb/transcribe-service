from contextlib import asynccontextmanager
from pathlib import Path

import anyio
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src import database
from src.api import health, transcriptions
from src.exceptions import install_error_handlers
from src.settings.config import settings
from src.settings.logger import configure_logging
from src.storage import prepare_storage
from src.transcription import worker

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await prepare_storage()
    try:
        await database.check_database()
        limiter = anyio.CapacityLimiter(settings.worker_concurrency)
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(worker.housekeeping)
            for _ in range(settings.worker_concurrency):
                tasks.start_soon(worker.poll, limiter)
            try:
                yield
            finally:
                tasks.cancel_scope.cancel()
    finally:
        await database.engine.dispose()


def create_app() -> FastAPI:
    configure_logging(settings.log_level)
    app = FastAPI(title="Transcribe Service", version="0.1.0", lifespan=lifespan)
    install_error_handlers(app)
    app.include_router(transcriptions.router)
    app.include_router(health.router)
    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(WEB_DIR / "index.html")

    return app
