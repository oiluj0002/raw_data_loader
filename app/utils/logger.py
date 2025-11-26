import logging
import sys


def get_logger(name: str = __name__) -> logging.Logger:
    """
    Initializes and returns a logger instance.

    Args:
        name: The name of the logger.

    Returns:
        A logger instance.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "[%(asctime)s] %(module)s %(levelname)s: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger
