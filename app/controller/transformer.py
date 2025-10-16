import pandas as pd
import pyarrow as pa

from utils.logger import get_logger

logger = get_logger()


class Transformer:
    """
    Applies basic transformations to a DataFrame chunk.
    """

    def __init__(
        self, pyarrow_schema: pa.Schema, deleted_columns: set[str] | None = None
    ) -> None:
        """
        Initializes the Transformer.
        Args:
            pyarrow_schema: The PyArrow schema to enforce on the data.
            deleted_columns: List of columns deleted in original database.
        """
        self.deleted_columns = deleted_columns
        self.pyarrow_schema = pyarrow_schema

    def transform_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies all necessary basic transformations to the DataFrame.
        Args:
            storage_client: An authenticated GCS storage client instance.
        """
        try:
            df = df.copy()

            # Transform deleted columns
            if self.deleted_columns:
                for col in self.deleted_columns:
                    df[col] = None

            # Transform timestamp[ns] columns to timestamp[ms]
            for field in self.pyarrow_schema:
                if pa.types.is_timestamp(field.type) and field.name in df.columns:
                    df[field.name] = df[field.name].astype("datetime64[ms, UTC]")

            logger.info("Chunk data transformed successfully")
            return df

        except Exception as e:
            logger.error(f"Failed to transform chunk based on data types. Error: {e}")
            raise
