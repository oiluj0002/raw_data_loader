import pandas as pd
from sqlalchemy import Engine
from typing import Generator

from utils.logger import get_logger
from config import env

logger = get_logger()


class PostgreSQLExtractor:
    """
    Extracts data from a PostgreSQL table in incremental chunks.

    This class connects to a PostgreSQL database using a SQLAlchemy engine
    and yields data in pandas DataFrames, suitable for processing large tables
    without loading the entire dataset into memory.
    """

    def __init__(self, engine: Engine) -> None:
        """
        Initializes the PostgreSQLExtractor.

        Args:
            engine: An authenticated SQLAlchemy engine instance for the PostgreSQL database.
        """
        self.engine = engine

        self.schema_name = env.DB_SCHEMA
        self.table_name = env.TABLE_NAME
        self.cursor_column = env.CURSOR_COLUMN

    def _build_incremental_query(self, last_cursor: str) -> str:
        """
        Builds the SQL query for incremental data extraction.

        This private method constructs a query that selects all new rows
        from the source table based on a cursor value.

        Args:
            last_cursor: The last recorded value of the cursor column, used to
                         fetch only newer records.

        Returns:
            A string containing the complete SQL query.
        """
        query = f"""
            SELECT *
            FROM {self.schema_name}.{self.table_name}
            WHERE {self.cursor_column} > '{last_cursor}'
            ORDER BY {self.cursor_column} ASC
            """
        return query

    def extract_chunks(
        self, last_cursor: str
    ) -> Generator[tuple[int, pd.DataFrame], None, None]:
        """
        Extracts data from the database and yields it in chunks.

        This is a generator method that executes the incremental query and
        yields each chunk as a pandas DataFrame.

        Args:
            last_cursor: The starting cursor value for the incremental query.

        Yields:
            A pandas DataFrame for each chunk of data fetched from the database.
        """
        query = self._build_incremental_query(last_cursor)

        logger.info(
            f"Starting to extract chunks from table: '{self.table_name}' using column: '{self.cursor_column}' as cursor"
        )
        with self.engine.connect() as conn:
            try:
                chunk_iterator = pd.read_sql(
                    sql=query, con=conn, chunksize=env.CHUNK_SIZE
                )

                for i, chunk in enumerate(chunk_iterator, 1):
                    if not chunk.empty:
                        try:
                            logger.info(f"Extracted chunk {i} with {len(chunk)} rows.")
                            yield i, chunk
                        except Exception:
                            logger.error("Error parsing iterator")
                            raise

            except Exception:
                logger.error("Error reading SQL Query")
                raise

            logger.info("Finished extracting all chunks from database.")
