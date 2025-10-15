import sys

from utils.logger import get_logger
from utils.metadata import CloudMetadataHelper
from utils.validation import build_pyarrow_schema

from core.db import get_db_engine
from core.gcp import get_storage_client
from core.schema import get_current_db_schema

from config import env

from controller.destination import GCSParquetLoader
from controller.source import PostgreSQLExtractor

logger = get_logger()


def main():
    logger.info("ETL application starting...")

    max_cursor_in_run = None
    total_rows = 0
    chunk_count = 0

    try:
        engine = get_db_engine()
        storage_client = get_storage_client()
        metadata = CloudMetadataHelper(storage_client)

        current_schema = get_current_db_schema(engine)
        metadata.save_reference_schema(current_schema)

        pyarrow_schema = build_pyarrow_schema(current_schema)

        extractor = PostgreSQLExtractor(engine)
        loader = GCSParquetLoader(storage_client, pyarrow_schema)

        last_cursor = metadata.get_last_cursor_value()

        for i, chunk in extractor.extract_chunks(last_cursor):
            loader.load_chunk(chunk, i)

            chunk_count += i
            total_rows += len(chunk)
            current_max = chunk[env.CURSOR_COLUMN].max()

            if max_cursor_in_run is None or current_max > max_cursor_in_run:
                max_cursor_in_run = current_max

        logger.info(
            f"Extraction-load loop finished. Processed {total_rows} rows in {chunk_count} chunks."
        )

        if max_cursor_in_run is not None:
            metadata.update_cursor_value(str(max_cursor_in_run))
        else:
            logger.info("No new data was processed in this run.")

        logger.info("ETL application finished successfully.")

    except Exception:
        logger.error("A fatal error occurred during the ETL process.", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
