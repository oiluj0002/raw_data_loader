import logging
from google.cloud import storage
from google.auth import exceptions

from config import env

logger = logging.getLogger(__name__)


def get_storage_client() -> storage.Client:
    """
    Creates and returns a Google Cloud Storage client instance.

    It relies on Application Default Credentials (ADC) for authentication,
    which is the standard way to authenticate in GCP environments.

    Returns:
        An authenticated GCS storage.Client instance.

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If authentication fails.
    """
    try:
        storage_client = storage.Client(project=env.GCP_PROJECT_ID)
        logger.info("Google Cloud Storage client created successfully.")

        return storage_client

    except exceptions.DefaultCredentialsError:
        logger.error(
            "Authentication with Google Cloud failed. "
            "Ensure you have run 'gcloud auth application-default login' "
            "for local development or that the service account has the "
            "correct permissions in production.",
            exc_info=True,
        )
        raise
