from celery import Celery
from celery.signals import setup_logging
from kombu import Queue

from src.settings.config import get_settings
from src.settings.logger import configure_logging

settings = get_settings()
celery_app = Celery("transcriber", broker=settings.broker_url, include=["src.transcription.tasks"])
celery_app.conf.update(
    task_default_queue=settings.queue_name,
    task_queues=[
        Queue(
            settings.queue_name,
            durable=True,
            queue_arguments={
                "x-max-length": settings.queue_max_length,
                "x-overflow": "reject-publish",
            },
        )
    ],
    task_ignore_result=True,
    task_acks_late=True,
    task_time_limit=settings.task_timeout_seconds,
    worker_concurrency=settings.worker_concurrency,
    worker_prefetch_multiplier=settings.worker_prefetch,
    worker_cancel_long_running_tasks_on_connection_loss=True,
    broker_connection_timeout=settings.broker_timeout_seconds,
    broker_heartbeat=settings.broker_heartbeat_seconds,
    # The database dispatcher owns retries; a publish failure must return promptly.
    broker_transport_options={"confirm_publish": True, "max_retries": 0},
    worker_hijack_root_logger=False,
    worker_enable_remote_control=False,
)


@setup_logging.connect
def setup_worker_logging(**kwargs):
    configure_logging(settings.log_level)
