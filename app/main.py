import sys

from config.manifest import get_manifest_json
from config import env

from core.db import get_db_engine
from core.gcp import get_storage_client

from utils.gcs_metadata import GCSMetadataManager
from utils.schema import (
    build_pyarrow_schema,
    get_current_db_schema,
    validate_current_schema,
)
from utils.logger import get_logger

from controller.loader import GCSParquetLoader
from controller.transformer import Transformer
from controller.extractor import SQLServerExtractor

logger = get_logger()


def main():
    """
    Main entry point for the incremental ETL application.

    This script orchestrates the process of extracting new or updated data from a
    SQL Server table, converting it to Parquet format, and loading it into a
    partitioned structure in Google Cloud Storage.

    The main workflow is as follows:
    1. Initialize connections to the database (SQL Server) and GCS.
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

        # List jobs for Cloud Run
        job_list = get_manifest_json(storage_client)
        if not (0 <= env.CLOUD_RUN_TASK_INDEX < len(job_list)):
            logger.error(
                f"Task index {env.CLOUD_RUN_TASK_INDEX} is out of bounds for manifest with {len(job_list)} items."
            )
            sys.exit(1)

        task_item = job_list[env.CLOUD_RUN_TASK_INDEX]
        schema_name = str(task_item.get("schema_name"))
        table_name = str(task_item.get("table_name"))
        cursor_column = str(task_item.get("cursor_column"))
        chunk_size = int(task_item.get("chunk_size", 1_000_000))

        # Fetch schemas
        metadata_manager = GCSMetadataManager(storage_client, table_name)
        reference_schema = metadata_manager.get_reference_schema()
        current_schema = get_current_db_schema(engine, schema_name, table_name)

        # Save reference schema if not exists
        if reference_schema is None:
            metadata_manager.save_reference_schema(current_schema)
            reference_schema = current_schema

        # Validate schema for Schema Drift detection
        schema_drift_info = validate_current_schema(reference_schema, current_schema)

        # Build pyarrow schema
        pyarrow_schema = build_pyarrow_schema(reference_schema)

        # I/O process
        extractor = SQLServerExtractor(
            engine=engine,
            columns_to_select=schema_drift_info.columns_to_select,
            schema_name=schema_name,
            table_name=table_name,
            cursor_column=cursor_column,
            chunk_size=chunk_size,
        )
        transformer = Transformer(pyarrow_schema, schema_drift_info.deleted_columns)
        loader = GCSParquetLoader(storage_client, pyarrow_schema, table_name)

        # Iterate in chunks
        last_cursor = metadata_manager.get_last_cursor_value()
        max_cursor_in_run = None
        total_rows = 0
        chunk_count = 0

        for i, chunk in extractor.extract_chunks(last_cursor):
            transformed_chunk = transformer.transform_chunk(chunk)
            loader.load_chunk(transformed_chunk, i)

            # Increment chunk counter by 1 per processed chunk
            chunk_count += 1
            total_rows += len(transformed_chunk)
            current_max = transformed_chunk[cursor_column].max()

            # Updates cursor
            if max_cursor_in_run is None or current_max > max_cursor_in_run:
                max_cursor_in_run = current_max

        logger.info(
            f"Extraction-load loop finished. Processed {total_rows} rows in {chunk_count} chunks."
        )

        # Saves cursor if cursor is altered in data batch
        if max_cursor_in_run is not None:
            cursor_to_save = max_cursor_in_run.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            metadata_manager.update_cursor_value(str(cursor_to_save))
        else:
            logger.info("No new data was processed in this run.")

        logger.info("ETL application finished successfully.")

    except Exception:
        logger.error("A fatal error occurred during the ETL process.", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
