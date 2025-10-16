import sys

from config import env

from core.db import get_db_engine
from core.gcp import get_storage_client

from utils.gcp_metadata import GCSMetadataManager
from utils.schema import (
    build_pyarrow_schema,
    get_current_db_schema,
    validate_current_schema,
)
from utils.logger import get_logger

from controller.loader import GCSParquetLoader
from controller.transformer import Transformer
from controller.extractor import PostgreSQLExtractor

logger = get_logger()


def main():
    """
    Main entry point for the incremental ETL application.

    This script orchestrates the process of extracting new or updated data from a
    PostgreSQL table, converting it to Parquet format, and loading it into a
    partitioned structure in Google Cloud Storage.

    The main workflow is as follows:
    1. Initialize connections to the database (PostgreSQL) and GCS.
    2. Manage schema: Fetches the current schema from the database and
       compares it with a saved reference schema in GCS to handle drift.
    3. Extract data in chunks using a cursor for incremental loads.
    4. Transforms data using basic business rules.
    5. Load each chunk into GCS as a Parquet file.
    6. Update the cursor value in GCS upon successful completion.
    """
    logger.info("ETL application starting...")

    max_cursor_in_run = None
    total_rows = 0
    chunk_count = 0

    try:
        # Initialization of instances
        engine = get_db_engine()
        storage_client = get_storage_client()
        metadata_manager = GCSMetadataManager(storage_client)

        # Fetch schemas
        reference_schema = metadata_manager.get_reference_schema()
        current_schema = get_current_db_schema(engine)

        # Save reference schema if not exists
        if reference_schema is None:
            metadata_manager.save_reference_schema(current_schema)
            reference_schema = current_schema

        # Validate schema for Schema Drift detection
        schema_drift_info = validate_current_schema(reference_schema, current_schema)

        # Build pyarrow schema
        pyarrow_schema = build_pyarrow_schema(reference_schema)

        # I/O process
        extractor = PostgreSQLExtractor(engine, schema_drift_info.columns_to_select)
        transformer = Transformer(pyarrow_schema, schema_drift_info.deleted_columns)
        loader = GCSParquetLoader(storage_client, pyarrow_schema)

        # Iterate in chunks
        last_cursor = metadata_manager.get_last_cursor_value()
        for i, chunk in extractor.extract_chunks(last_cursor):
            transformed_chunk = transformer.transform_chunk(chunk)
            loader.load_chunk(transformed_chunk, i)

            chunk_count += i
            total_rows += len(chunk)
            current_max = chunk[
                env.CURSOR_COLUMN
            ].max()  # preserves max original [ns] timestamp type

            # Updates cursor
            if max_cursor_in_run is None or current_max > max_cursor_in_run:
                max_cursor_in_run = current_max

        logger.info(
            f"Extraction-load loop finished. Processed {total_rows} rows in {chunk_count} chunks."
        )

        # Saves cursor if cursor is altered in data batch
        if max_cursor_in_run is not None:
            metadata_manager.update_cursor_value(str(max_cursor_in_run))
        else:
            logger.info("No new data was processed in this run.")

        logger.info("ETL application finished successfully.")

    except Exception:
        logger.error("A fatal error occurred during the ETL process.", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
