import logging
import sys

from pythonjsonlogger.json import JsonFormatter


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter(
            ["levelname", "name", "message"],
            rename_fields={
                "levelname": "level",
                "name": "logger",
                "message": "event",
                "exc_info": "exception",
            },
            timestamp=True,
            json_ensure_ascii=False,
        )
    )
    logging.basicConfig(level=level, handlers=[handler], force=True)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(level)
