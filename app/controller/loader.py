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
        self, storage_client: storage.Client, pyarrow_schema: pa.Schema, table_name: str
    ) -> None:
        """
        Initializes the Loader.

        Args:
            storage_client: An authenticated GCS storage client instance.
            pyarrow_schema: The PyArrow schema to enforce on the data.
            table_name: The name of the table to which the data belongs.
        """
        self.storage_client = storage_client
        self.pyarrow_schema = pyarrow_schema
        self.table_name = table_name
        self.bucket_name = env.GCS_BUCKET_NAME
        self.execution_ts = env.EXECUTION_TS

    def load_chunk(self, df: pd.DataFrame, chunk_index: int):
        """
        Loads a DataFrame chunk to GCS as a Parquet file.

        Args:
            df: The DataFrame chunk to load.
            chunk_index: The index of the chunk, used for naming the output file.
        """
        try:
            df = df.filter(items=self.pyarrow_schema.names)
            table = pa.Table.from_pandas(
                df, schema=self.pyarrow_schema, preserve_index=False, safe=True
            )

            # Date Logic
            ts: datetime
            if self.execution_ts:
                try:
                    ts = datetime.fromisoformat(self.execution_ts)
                    logger.info(f"Using Airflow timestamp: {self.execution_ts}")
                except (ValueError, TypeError):
                    logger.error(
                        f"Invalid timestamp received: {self.execution_ts}. Using 'Now'."
                    )
                    ts = datetime.now(timezone.utc)
            else:
                ts = datetime.now(timezone.utc)
                logger.warning("Execution timestamp not entered, Using 'Now'.")

            if ts.tzinfo is None:
                ts = ts.astimezone(timezone.utc)

            # Partition Logic
            gcs_path = (
                f"mssql/tables/{self.table_name}/ingestion/"
                f"year={ts.year:04d}/"
                f"month={ts.month:02d}/"
                f"day={ts.day:02d}/"
                f"hour={ts.hour:02d}/"
                f"{ts.strftime('%Y%m%d%H%M%S')}_{chunk_index}.parquet"
            )
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)

            # Write Logic
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)
            blob.upload_from_file(buffer, content_type="application/parquet")

            logger.info(
                f"Chunk {chunk_index} successfully loaded to: gs://{self.bucket_name}/{gcs_path}"
            )

        except Exception as e:
            logger.error(f"Failed to load chunk {chunk_index} to GCS. Error: {e}")
            raise
