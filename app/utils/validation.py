import pyarrow as pa

from utils.logger import get_logger
from config import env

logger = get_logger()


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
