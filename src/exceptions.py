import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from starlette.exceptions import HTTPException

from src.schemas import ErrorDetail, ErrorResponse

log = logging.getLogger(__name__)


class AppError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400):
        super().__init__(message)
        self.code, self.message, self.status_code = code, message, status_code


class StorageFullError(AppError):
    def __init__(self):
        super().__init__("storage_full", "Storage is temporarily full. Please try later.", 503)


def error_response(code: str, message: str, status_code: int) -> JSONResponse:
    payload = ErrorResponse(error=ErrorDetail(code=code, message=message))
    return JSONResponse(payload.model_dump(mode="json"), status_code=status_code)


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def application_error(request: Request, exc: AppError):
        return error_response(exc.code, exc.message, exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        return error_response("invalid_request", "Check the request fields and job ID.", 422)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException):
        response = error_response("http_error", str(exc.detail), exc.status_code)
        response.headers.update(exc.headers or {})
        return response

    def unavailable(request: Request, exc: Exception):
        log.warning("database_unavailable")
        return error_response("service_unavailable", "Please try again shortly.", 503)

    app.add_exception_handler(OperationalError, unavailable)
    app.add_exception_handler(PoolTimeout, unavailable)

    @app.exception_handler(Exception)
    def unexpected(request: Request, exc: Exception):
        # Starlette re-raises unexpected errors after the response; Uvicorn logs the traceback.
        return error_response("internal_error", "The request could not be completed.", 500)
