import pandas as pd
import pyarrow as pa
from decimal import Decimal
from cryptography.fernet import Fernet

from utils.logger import get_logger
from config import env

logger = get_logger()

SENSITIVE_COLUMNS = {
    "nr_cpf",
    "nm_ip",
    "nr_celular",
    "nm_email_completo",
}


class Transformer:
    """
    Applies basic transformations to a DataFrame chunk.
    """

    def __init__(
        self,
        pyarrow_schema: pa.Schema,
        deleted_columns: set[str] | None = None,
    ) -> None:
        """
        Initializes the Transformer.
        Args:
            pyarrow_schema: The PyArrow schema to enforce on the data.
            deleted_columns: List of columns deleted in original database. Defaults to None
        """
        self.deleted_columns = deleted_columns
        self.pyarrow_schema = pyarrow_schema
        self.fernet = Fernet(env.SECRET_FERNET_KEY)

    def transform_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies all necessary basic transformations to the DataFrame.

        Args:
            df: The DataFrame chunk to be transformed.
        """
        try:
            df = df.copy()

            # Transform deleted columns
            if self.deleted_columns:
                for col in self.deleted_columns:
                    df[col] = None

            for field in self.pyarrow_schema:
                # Transform timestamp columns to millisecond precision safely
                if pa.types.is_timestamp(field.type) and field.name in df.columns:
                    df[field.name] = pd.to_datetime(
                        df[field.name], errors="coerce"
                    ).astype("datetime64[ms]")

                # Transform float to high precision decimal
                elif pa.types.is_decimal(field.type) and field.name in df.columns:
                    # Use map for element-wise conversion to satisfy type checkers and avoid overload issues
                    df[field.name] = df[field.name].map(
                        lambda x: Decimal(str(x)) if pd.notna(x) else None
                    )

            # Encrypt sensitive columns
            for column in df.columns:
                if column in SENSITIVE_COLUMNS:
                    df[column] = df[column].map(
                        lambda x: (
                            self.fernet.encrypt(str(x).encode()).decode()
                            if pd.notna(x)
                            else None
                        )
                    )

            logger.info("Chunk data transformed successfully")
            return df

        except Exception as e:
            logger.error(f"Failed to transform chunk based on data types. Error: {e}")
            raise
