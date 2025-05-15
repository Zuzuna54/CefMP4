import logging
import sys
import structlog
from src.config import settings


def setup_logging():
    """Configures structlog for structured JSON logging."""
    log_level_str = settings.log_level.upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.StackInfoRenderer(),  # For stack traces
        structlog.dev.set_exc_info,  # For exception details
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    if settings.app_env == "development":
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),  # Pretty printing for dev
        ]
    else:
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,  # For JSON tracebacks
            structlog.processors.JSONRenderer(),  # Render to JSON
        ]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure standard logging to use structlog
    root_logger = logging.getLogger()
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)  # Or sys.stderr
    # No formatter needed here for handler if structlog handles it all
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    # Mute noisy loggers
    logging.getLogger("watchfiles").setLevel(logging.INFO)
    logging.getLogger("asyncio").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("aiobotocore").setLevel(logging.WARNING)
    # logging.getLogger("s3transfer").setLevel(logging.WARNING) # Already quite high level

    logger = structlog.get_logger("logging_config")
    logger.info(
        "Structlog configured", log_level=log_level_str, app_env=settings.app_env
    )
