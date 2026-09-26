from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from src.api import health, transcriptions
from src.database import check_database, close_database
from src.exceptions import install_error_handlers
from src.settings.config import get_settings
from src.settings.logger import configure_logging
from src.storage import prepare_storage

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(prepare_storage)
    try:
        await run_in_threadpool(check_database)
        yield
    finally:
        await run_in_threadpool(close_database)


def create_app() -> FastAPI:
    configure_logging(get_settings().log_level)
    app = FastAPI(title="Transcribe Service", version="0.1.0", lifespan=lifespan)
    install_error_handlers(app)
    app.include_router(transcriptions.router)
    app.include_router(health.router)
    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(WEB_DIR / "index.html")

    return app
