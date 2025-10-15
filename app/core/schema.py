from sqlalchemy import Engine, inspect

from utils.logger import get_logger
from config import env

logger = get_logger()


def get_current_db_schema(engine: Engine) -> dict[str, str]:
    """
    Inspects the database and returns the current table schema.

    This method uses the engine provided during initialization and
    configuration from the global 'env' object to fetch schema details.

    Args:
        engine: SQLAlchemy Engine instance.

    Returns:
        A dictionary mapping column names to their SQL type as a string.
    """
    table_name = env.TABLE_NAME
    schema_name = env.DB_SCHEMA

    inspector = inspect(engine)
    columns = {
        col["name"]: str(col["type"])
        for col in inspector.get_columns(table_name, schema=schema_name)
    }
    logger.info(
        f"Fetched current database schema for {schema_name}.{table_name} with {len(columns)} columns."
    )
    return columns
