from google.cloud import storage
import json

from utils.logger import get_logger
from config import env

logger = get_logger()


class GCSMetadataManager:
    """
    A class to fetch and manage metadata for a data job from GCS.

    This class handles interactions with GCS to retrieve the last processed
    cursor value and the saved table schema, which are essential for
    incremental loads and schema validation.
    """

    def __init__(self, storage_client: storage.Client, table_name: str) -> None:
        """
        Initializes the GCSMetadataManager for a specific table.

        Args:
            storage_client: An authenticated GCS storage client instance.
            table_name: The name of the table for which to manage metadata.
        """
        self.storage_client = storage_client
        self.table_name = table_name
        self.bucket_name = env.GCS_BUCKET_NAME

        # Derive file paths from the table name
        cursor_file_path = (
            f"mssql/tables/{self.table_name}/state/{self.table_name}_cursor.txt"
        )
        schema_file_path = (
            f"mssql/tables/{self.table_name}/state/{self.table_name}_schema.json"
        )

        # The instance now holds direct references to the GCS blobs
        self.cursor_blob = self._get_gcs_blob(cursor_file_path)
        self.schema_blob = self._get_gcs_blob(schema_file_path)

    def _get_gcs_blob(self, file_path: str) -> storage.Blob:
        """
        Internal helper to get a GCS blob object.

        Args:
            file_path: The path to the file within the GCS bucket.

        Returns:
            A GCS Blob object.
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        return bucket.blob(file_path)

    def get_last_cursor_value(self) -> str:
        """
        Fetches the last cursor value from the state file in GCS.

        If the file does not exist, it returns a default historical timestamp,
        signaling an initial full load.

        Returns:
            The last cursor value as a string.
        """
        try:
            last_cursor = self.cursor_blob.download_as_string().decode("utf-8")
            logger.info(f"Found last cursor value: {last_cursor}")
            return last_cursor
        except Exception:
            logger.warning("Cursor file not found. Assuming initial load.")
            return "1900-01-01 00:00:00.000"

    def get_reference_schema(self) -> dict[str, str] | None:
        """
        Loads the reference schema from its JSON file in GCS.

        If the file does not exist, it returns None, signaling that a new
        reference schema should be created.

        Returns:
            A dictionary representing the table schema, or None if not found.
        """
        try:
            schema_json = self.schema_blob.download_as_string().decode("utf-8")
            schema = json.loads(schema_json)
            logger.info(f"Reference schema loaded with {len(schema)} columns.")
            return schema
        except Exception:
            logger.warning("No reference schema found. A new one should be created.")
            return None

    def save_reference_schema(self, schema: dict[str, str]) -> None:
        """
        Saves the provided schema as the new reference schema in GCS.

        Args:
            schema: The schema dictionary to save as a JSON file.
        """
        schema_json = json.dumps(schema, indent=4)
        self.schema_blob.upload_from_string(
            schema_json, content_type="application/json"
        )
        logger.info(
            f"Reference schema saved to: gs://{self.bucket_name}/{self.schema_blob.name}"
        )

    def update_cursor_value(self, new_cursor_value: str):
        """
        Updates the cursor file in GCS with the new latest value.

        Args:
            new_cursor_value: The new cursor value to save.
        """
        if not new_cursor_value:
            logger.warning("No new cursor value provided to update. Skipping.")
            return

        self.cursor_blob.upload_from_string(str(new_cursor_value))
        logger.info(f"Cursor file updated with new value: {new_cursor_value}")
