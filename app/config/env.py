import os
from dotenv import load_dotenv
from utils.logger import get_logger
from typing import Any

load_dotenv("secret/.env")
logger = get_logger()


def _get_required_env(var_name: Any) -> Any:
    """
    Collects required environment variables and checks if they exists.

    Args:
        var_name: Environment variable name.

    Returns:
        value: Environment variable value.

    """
    value = os.getenv(var_name)
    if not value:
        logger.error(f"Environment variable not found: {var_name}.")
        raise EnvironmentError(f"Environment variable {var_name} not defined.")
    return value


def _get_optional_env(var_name: str, default: Any = None) -> Any:
    """
    Collects optional environment variables and checks if they exists.

    Args:
        var_name: Environment variable name.
        default: Default value if Environment variable value is null or not specified.

    Returns:
        value: Environment variable value.

    """
    return os.getenv(var_name, default)


# --- Database ---
DB_USER = _get_required_env("DB_USER")
DB_PASSWORD = _get_required_env("DB_PASSWORD")
DB_HOST = _get_required_env("DB_HOST")
DB_PORT = _get_required_env("DB_PORT")
DB_NAME = _get_required_env("DB_NAME")
DB_SCHEMA = _get_optional_env("DB_SCHEMA", "dbo")

# --- GCP ---
GCP_PROJECT_ID = _get_required_env("GCP_PROJECT_ID")
GCS_BUCKET_NAME = _get_required_env("GCS_BUCKET_NAME")

# --- Extraction Values ---
TABLE_NAME = _get_required_env("TABLE_NAME")
CURSOR_COLUMN = _get_required_env("CURSOR_COLUMN")
try:
    CHUNK_SIZE = int(_get_optional_env("CHUNK_SIZE", 100000))
except (ValueError, TypeError):
    logger.warning("Invalid CHUNK_SIZE provided, defaulting to 100000.")
    CHUNK_SIZE = 100000
