from sqlalchemy import Engine, inspect
import pyarrow as pa
from dataclasses import dataclass

from utils.logger import get_logger
from config import env

logger = get_logger()


@dataclass
class SchemaDriftInfo:
    """Data container for informations of schema drift."""

    new_columns: set[str]
    deleted_columns: set[str]
    columns_to_select: list[str]


def _map_sql_to_pyarrow_dtype(sql_type: str) -> pa.DataType:
    """
    Maps SQL data types to PyArrow data types.

    Args:
        sql_type: SQL data type.

    Returns:
        PyArrow data type.

    """
    sql_type_lower = sql_type.lower()

    if any(s in sql_type_lower for s in ["int", "smallint", "tinyint", "bigint"]):
        return pa.int64()
    elif any(s in sql_type_lower for s in ["decimal", "numeric", "money"]):
        return pa.decimal128(38, 9)
    elif any(s in sql_type_lower for s in ["float", "real"]):
        return pa.float64()
    elif "bit" in sql_type_lower:
        return pa.bool_()
    elif any(s in sql_type_lower for s in ["timestamp", "datetime"]):
        return pa.timestamp("ms")
    elif "date" in sql_type_lower:
        return pa.date32()
    else:  # Fallback for string types like 'char', 'text', 'uuid', etc.
        return pa.string()


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


def validate_current_schema(
    reference: dict[str, str], current: dict[str, str]
) -> SchemaDriftInfo:
    reference_schema_cols = set(reference.keys())
    current_schema_cols = set(current.keys())

    new_cols = current_schema_cols - reference_schema_cols
    if new_cols:
        logger.warning(
            f"[SCHEMA DRIFT] New columns detected and ignored: '{sorted(list(new_cols))}'"
        )

    deleted_cols = reference_schema_cols - current_schema_cols
    if deleted_cols:
        logger.warning(
            f"[SCHEMA DRIFT] Deleted columns detected: '{sorted(list(deleted_cols))}', will be added as null: "
        )

    cols_to_select = sorted(
        list(reference_schema_cols.intersection(current_schema_cols))
    )

    return SchemaDriftInfo(
        new_columns=new_cols,
        deleted_columns=deleted_cols,
        columns_to_select=cols_to_select,
    )


def build_pyarrow_schema(columns: dict[str, str]) -> pa.Schema:
    """
    Builds a PyArrow Schema object from the current database schema.

    Args:
        engine: SQLAlchemy Engine instance.

    Returns:
        A PyArrow Schema object representing the table's structure.
    """
    pa_schema = pa.schema(
        [
            (col_name, _map_sql_to_pyarrow_dtype(sql_type))
            for col_name, sql_type in columns.items()
        ]
    )

    logger.info(f"Successfully built PyArrow schema for table {env.TABLE_NAME}.")
    return pa_schema
