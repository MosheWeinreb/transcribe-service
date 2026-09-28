"""Run the API with `python main.py`, or import `main:app` with Uvicorn."""

import uvicorn

from src.app import create_app
from src.settings.config import settings

app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=settings.server_host,
        port=settings.server_port,
        log_config=None,
    )
