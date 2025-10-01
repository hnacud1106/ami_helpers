import logging
import sys
from logging.config import dictConfig

def setup_logging(level: str = "INFO", logfile: str | None = None) -> None:
    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "console",
            "stream": "ext://sys.stdout",
        }
    }

    if logfile:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "formatter": "file",
            "filename": logfile,
            "encoding": "utf-8",
        }

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "console": {
                "format": "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
            },
            "file": {
                "format": "%(asctime)s %(levelname)s %(name)s - %(message)s"
            },
        },
        "handlers": handlers,
        "root": {
            "level": level.upper(),
            "handlers": list(handlers.keys()),
        },
        "loggers": {
            "stomp.py": {"level": "WARN"},
            "elasticsearch": {"level": "WARN"},
            "pika": {"level": "WARNING"},
            "celery.app.trace": {"level": "WARNING"},
            "py4j": {"level": "WARN"},
            "httpx": {"level": "WARN"},
        },
    }

    dictConfig(config)


if __name__ == "__main__":
    setup_logging("DEBUG", "app.log")
    logger = logging.getLogger("demo")

    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
