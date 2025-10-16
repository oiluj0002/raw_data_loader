import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timezone
from google.cloud import storage

from utils.logger import get_logger
from config import env

logger = get_logger()


class GCSParquetLoader:
    """
    Loads DataFrames to GCS as Parquet files using the explicit
    google-cloud-storage API.
    """

    def __init__(
        self,
        storage_client: storage.Client,
        pyarrow_schema: pa.Schema,
    ) -> None:
        """
        Initializes the Loader.

        Args:
            storage_client: An authenticated GCS storage client instance.
            pyarrow_schema: The PyArrow schema to enforce on the data.
        """
        self.storage_client = storage_client
        self.pyarrow_schema = pyarrow_schema
        self.bucket_name = env.GCS_BUCKET_NAME
        self.table_name = env.TABLE_NAME

    def load_chunk(self, df: pd.DataFrame, chunk_index: int):
        try:
            df = df.filter(items=self.pyarrow_schema.names)
            table = pa.Table.from_pandas(
                df, schema=self.pyarrow_schema, preserve_index=False, safe=True
            )

            now = datetime.now(timezone.utc)
            gcs_path = (
                f"postgresql/{env.TABLE_NAME}/ingestion/"
                f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/"
                f"{now.strftime('%Y%m%d%H%M%S')}_{chunk_index}.parquet"
            )
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)

            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            blob.upload_from_file(buffer, content_type="application/parquet")

            logger.info(
                f"Chunk {chunk_index} successfully loaded to: gs://{self.bucket_name}/{gcs_path}"
            )

        except Exception as e:
            logger.error(f"Failed to load chunk {chunk_index} to GCS. Error: {e}")
            raise
