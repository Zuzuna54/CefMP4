import pytest
import logging
import sys
import structlog
from unittest.mock import patch, MagicMock
from src.logging_config import setup_logging


@pytest.fixture
def reset_logging():
    """Reset logging configuration after each test."""
    # Store original configuration
    original_handlers = logging.root.handlers.copy()
    original_level = logging.root.level

    # Instead of accessing internal structlog config, we'll just re-configure it later
    yield

    # Restore original configuration
    logging.root.handlers = original_handlers
    logging.root.setLevel(original_level)

    # Reset structlog by re-configuring with default values
    # This is simpler than trying to access/store internal config
    structlog.configure(
        processors=[structlog.processors.JSONRenderer()],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def test_development_logging(reset_logging):
    """Test logging configuration in development environment."""
    with patch("src.logging_config.settings") as mock_settings:
        mock_settings.app_env = "development"
        mock_settings.log_level = "debug"

        with patch("structlog.configure") as mock_configure:
            setup_logging()

            # Check structlog was configured with ConsoleRenderer
            call_args = mock_configure.call_args[1]
            processors = call_args["processors"]

            # Check for ConsoleRenderer in processors
            console_renderer_found = any(
                isinstance(p, structlog.dev.ConsoleRenderer)
                or p == structlog.dev.ConsoleRenderer()
                or (
                    callable(p)
                    and p.__module__ == structlog.dev.ConsoleRenderer.__module__
                )
                for p in processors
            )

            assert console_renderer_found, "Development mode should use ConsoleRenderer"

            # Verify log level was set to DEBUG
            assert logging.root.level == logging.DEBUG


def test_production_logging(reset_logging):
    """Test logging configuration in production environment."""
    with patch("src.logging_config.settings") as mock_settings:
        mock_settings.app_env = "production"
        mock_settings.log_level = "info"

        with patch("structlog.configure") as mock_configure:
            setup_logging()

            # Check structlog was configured with JSONRenderer
            call_args = mock_configure.call_args[1]
            processors = call_args["processors"]

            # Check for JSONRenderer in processors
            json_renderer_found = any(
                p == structlog.processors.JSONRenderer()
                or (
                    callable(p)
                    and p.__module__ == structlog.processors.JSONRenderer.__module__
                )
                for p in processors
            )

            assert json_renderer_found, "Production mode should use JSONRenderer"

            # Verify log level was set to INFO
            assert logging.root.level == logging.INFO


def test_log_level_configuration(reset_logging):
    """Test different log levels are correctly applied."""
    with patch("src.logging_config.settings") as mock_settings:
        mock_settings.app_env = "production"

        # Test different log levels
        log_levels = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }

        for level_str, level_value in log_levels.items():
            mock_settings.log_level = level_str
            setup_logging()
            assert (
                logging.root.level == level_value
            ), f"Log level should be {level_str.upper()}"


def test_stdout_handler(reset_logging):
    """Test that a StreamHandler for stdout is added."""
    with patch("src.logging_config.settings") as mock_settings:
        mock_settings.app_env = "development"
        mock_settings.log_level = "info"

        setup_logging()

        # Check if a StreamHandler with stdout is in root logger handlers
        stdout_handlers = [
            h
            for h in logging.root.handlers
            if isinstance(h, logging.StreamHandler) and h.stream == sys.stdout
        ]

        assert len(stdout_handlers) > 0, "Should add a StreamHandler with stdout"


def test_noisy_loggers_are_silenced(reset_logging):
    """Test that noisy modules are set to appropriate log levels."""
    with patch("src.logging_config.settings") as mock_settings:
        mock_settings.app_env = "development"
        mock_settings.log_level = "debug"

        setup_logging()

        # Check log levels for noisy modules
        assert logging.getLogger("watchfiles").level == logging.INFO
        assert logging.getLogger("asyncio").level == logging.INFO
        assert logging.getLogger("botocore").level == logging.WARNING
        assert logging.getLogger("aiobotocore").level == logging.WARNING
