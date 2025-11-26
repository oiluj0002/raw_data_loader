import pyarrow as pa
from pytest_mock import MockerFixture


from app.utils.schema import (
    _map_sql_to_pyarrow_dtype,
    validate_current_schema,
    build_pyarrow_schema,
    get_current_db_schema,
)


def test_map_sql_to_pyarrow_dtype_various_types():
    """Tests the mapping for common SQL types."""
    assert _map_sql_to_pyarrow_dtype("INT") == pa.int64()
    assert _map_sql_to_pyarrow_dtype("bigint") == pa.int64()
    assert _map_sql_to_pyarrow_dtype("SMALLINT") == pa.int64()
    assert _map_sql_to_pyarrow_dtype("tinyint") == pa.int64()
    assert _map_sql_to_pyarrow_dtype("VARCHAR(50)") == pa.string()
    assert _map_sql_to_pyarrow_dtype("NVARCHAR(MAX)") == pa.string()
    assert _map_sql_to_pyarrow_dtype("TEXT") == pa.string()
    assert _map_sql_to_pyarrow_dtype("DECIMAL(18,2)") == pa.decimal128(38, 9)
    assert _map_sql_to_pyarrow_dtype("numeric") == pa.decimal128(38, 9)
    assert _map_sql_to_pyarrow_dtype("money") == pa.decimal128(38, 9)
    assert _map_sql_to_pyarrow_dtype("FLOAT") == pa.float64()
    assert _map_sql_to_pyarrow_dtype("real") == pa.float64()
    assert _map_sql_to_pyarrow_dtype("BIT") == pa.bool_()
    assert _map_sql_to_pyarrow_dtype("timestamp") == pa.timestamp("ms")
    assert _map_sql_to_pyarrow_dtype("DATETIME2(3)") == pa.timestamp("ms")
    assert _map_sql_to_pyarrow_dtype("datetime") == pa.timestamp("ms")
    assert _map_sql_to_pyarrow_dtype("DATE") == pa.date32()
    assert _map_sql_to_pyarrow_dtype("UUID") == pa.string()


def test_map_sql_to_pyarrow_dtype_fallback(mocker):
    """Tests the fallback to string for unmapped types and logs a warning."""
    mock_logger_warning = mocker.patch("app.utils.schema.logger.warning")
    assert _map_sql_to_pyarrow_dtype("XML") == pa.string()
    assert _map_sql_to_pyarrow_dtype("JSON") == pa.string()
    assert _map_sql_to_pyarrow_dtype("UUID") == pa.string()
    assert _map_sql_to_pyarrow_dtype("TEXT") == pa.string()
    # Check if logger.warning was called
    assert (
        mock_logger_warning.call_count >= 1
    )  # At least one warning expected if not mapped


def test_validate_current_schema_basic(mocker: MockerFixture):
    """Validate detection of new, deleted and intersecting columns and warnings."""
    mock_warn = mocker.patch("app.utils.schema.logger.warning")

    reference = {"id": "INT", "amount": "DECIMAL(18,2)", "ts": "DATETIME2(3)"}
    current = {"id": "INT", "amount": "DECIMAL(18,2)", "extra": "VARCHAR(10)"}

    info = validate_current_schema(reference, current)

    assert info.new_columns == {"extra"}
    assert info.deleted_columns == {"ts"}
    assert info.columns_to_select == ["amount", "id"]  # sorted intersection

    # Should warn once for new and once for deleted
    assert mock_warn.call_count >= 2


def test_build_pyarrow_schema_constructs_expected():
    """Ensure build_pyarrow_schema maps SQL types to expected PyArrow schema."""
    cols = {
        "id": "INT",
        "price": "DECIMAL(18,2)",
        "flag": "BIT",
        "event_time": "DATETIME2(3)",
        "created_date": "DATE",
        "name": "VARCHAR(50)",
    }

    schema = build_pyarrow_schema(cols)

    expected = pa.schema(
        [
            ("id", pa.int64()),
            ("price", pa.decimal128(38, 9)),
            ("flag", pa.bool_()),
            ("event_time", pa.timestamp("ms")),
            ("created_date", pa.date32()),
            ("name", pa.string()),
        ]
    )

    assert schema == expected


def test_get_current_db_schema_uses_inspector(mocker: MockerFixture):
    """Mock SQLAlchemy inspect to verify columns are returned as name->type string map."""
    mock_inspect = mocker.patch("app.utils.schema.inspect")
    mock_logger_info = mocker.patch("app.utils.schema.logger.info")

    fake_inspector = mocker.MagicMock()
    fake_inspector.get_columns.return_value = [
        {"name": "id", "type": "INTEGER"},
        {"name": "amount", "type": "DECIMAL(18,2)"},
    ]
    mock_inspect.return_value = fake_inspector

    engine = mocker.MagicMock()
    result = get_current_db_schema(engine, "dbo", "payments")

    assert result == {"id": "INTEGER", "amount": "DECIMAL(18,2)"}
    fake_inspector.get_columns.assert_called_once_with("payments", schema="dbo")
    mock_logger_info.assert_called()


def test_validate_current_schema_no_drift(mocker: MockerFixture):
    """When schemas match exactly, no warnings and correct selection order."""
    mock_warn = mocker.patch("app.utils.schema.logger.warning")

    reference = {"a": "INT", "b": "VARCHAR(10)", "c": "DATE"}
    current = {"a": "INT", "b": "VARCHAR(10)", "c": "DATE"}

    info = validate_current_schema(reference, current)

    assert info.new_columns == set()
    assert info.deleted_columns == set()
    assert info.columns_to_select == ["a", "b", "c"]
    mock_warn.assert_not_called()


def test_build_pyarrow_schema_preserves_order():
    """Schema should preserve dict insertion order of columns."""
    cols = {"b": "INT", "a": "INT", "c": "INT"}
    schema = build_pyarrow_schema(cols)
    assert schema.names == ["b", "a", "c"]
