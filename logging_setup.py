import logging
import os
from logging.handlers import TimedRotatingFileHandler


_DEFAULT_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | "
    "%(message)s"
)


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def setup_logging(service_name: str, *, log_dir: str | None = None) -> logging.Logger:
    """Configure console + rotating file logging.

    Environment variables:
      - LOG_LEVEL: DEBUG|INFO|WARNING|ERROR (default: INFO)
      - LOG_DIR: directory for log files (default: ./logs)

    File name pattern: <service_name>.log
    Rotation: daily, keep 14 days
    """

    level_name = (os.getenv("LOG_LEVEL") or "INFO").upper().strip()
    level = getattr(logging, level_name, logging.INFO)

    effective_log_dir = log_dir or os.getenv("LOG_DIR") or os.path.join(os.getcwd(), "logs")
    _ensure_dir(effective_log_dir)

    logger = logging.getLogger(service_name)
    logger.setLevel(level)
    logger.propagate = False

    # Avoid duplicate handlers if hot-reload imports multiple times.
    if logger.handlers:
        return logger

    formatter = logging.Formatter(_DEFAULT_FORMAT)

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(formatter)

    file_path = os.path.join(effective_log_dir, f"{service_name}.log")
    file_handler = TimedRotatingFileHandler(
        file_path,
        when="midnight",
        interval=1,
        backupCount=int(os.getenv("LOG_ROTATION_DAYS") or 14),
        encoding="utf-8",
        utc=True,
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    logger.info("Logging initialized | level=%s | file=%s", level_name, file_path)
    return logger
