from google.cloud import storage
import json
from typing import List, Dict

from config import env
from utils.logger import get_logger

logger = get_logger()


def get_manifest_json(storage_client: storage.Client) -> List[Dict]:
    """Loads and parses manifest file in JSON format from GCS.

    Args:
        storage_client: An authenticated GCS storage client instance.

    Returns:
        A list of dictionaries, where each dictionary represents a job.
    """
    file_path = "mssql/manifest.json"
    bucket_name = env.GCS_BUCKET_NAME

    try:
        manifest_blob = storage_client.bucket(bucket_name).blob(file_path)
        job_list_json = manifest_blob.download_as_string().decode("utf-8")
        job_list = json.loads(job_list_json)
        logger.info(f"Manifest loaded with {len(job_list)} jobs.")
        return job_list

    except Exception:
        logger.error("Error reading manifest from GCS.", exc_info=True)
        raise
