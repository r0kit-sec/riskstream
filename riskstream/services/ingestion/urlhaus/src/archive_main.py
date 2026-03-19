import logging
import os
import sys
from pathlib import Path

from client import JsonFormatter
from feed_store import run_archive_lifecycle

REPO_ROOT = Path(__file__).resolve().parents[5]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


logger = logging.getLogger("urlhaus.archive")


def configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level)


def log_event(level: int, message: str, **fields) -> None:
    logger.log(level, message, extra={"fields": fields})


def run() -> None:
    configure_logging()
    environment = os.getenv("ENVIRONMENT", "unknown")
    hot_retention_days = int(os.getenv("URLHAUS_HOT_RETENTION_DAYS", "30"))
    archive_retention_days = int(os.getenv("URLHAUS_ARCHIVE_RETENTION_DAYS", "180"))

    lifecycle = run_archive_lifecycle(
        hot_retention_days=hot_retention_days,
        archive_retention_days=archive_retention_days,
    )
    log_event(
        logging.INFO,
        "URLhaus archive lifecycle completed",
        service="urlhaus-archive-lifecycle",
        event="archive_lifecycle_completed",
        environment=environment,
        **lifecycle,
    )


if __name__ == "__main__":
    run()
