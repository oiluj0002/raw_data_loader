import os

try:
    # Load variables from a .env file when available (no hard dependency in prod)
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

_REQUIRED_VARS = [
    "DB_USER",
    "DB_PASSWORD",
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "GCP_PROJECT_ID",
    "GCS_BUCKET_NAME",
    "SECRET_FERNET_KEY"
]

_missing = [name for name in _REQUIRED_VARS if not os.getenv(name)]
if _missing:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(_missing)}"
    )

# --- Database ---
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]

# --- GCP ---
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
SECRET_FERNET_KEY = bytes(os.environ["SECRET_FERNET_KEY"], "utf-8")

# --- Extraction Values with Defaults ---
EXECUTION_TS = os.getenv("EXECUTION_TS", "1900-01-01 00:00:00.000000")
CLOUD_RUN_TASK_INDEX = int(os.getenv("CLOUD_RUN_TASK_INDEX", "0"))
