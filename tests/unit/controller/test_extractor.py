import pandas as pd
import pytest
from pytest_mock import MockerFixture
from unittest.mock import MagicMock

from app.controller.extractor import SQLServerExtractor


@pytest.fixture
def mock_engine(mocker: MockerFixture) -> MagicMock:
    """Provides a mock SQLAlchemy engine."""
    return mocker.MagicMock()


def test_build_incremental_query(mock_engine: MagicMock):
    """
    Tests if the incremental SQL query is built correctly.
    """
    extractor = SQLServerExtractor(
        engine=mock_engine,
        columns_to_select=["id", "name"],
        schema_name="dbo",
        table_name="users",
        cursor_column="id",
    )
    query = extractor._build_incremental_query()

    assert "SELECT [id], [name]" in query
    assert "FROM [dbo].[users]" in query
    assert "WHERE [id] > ?" in query
    assert "ORDER BY [id] ASC" in query


def test_extract_chunks_yields_data(mock_engine: MagicMock, mocker: MockerFixture):
    """
    Tests that the extractor yields chunks correctly when data is available.
    """
    # Mock the return value of pd.read_sql
    df1 = pd.DataFrame({"id": [1, 2]})
    df2 = pd.DataFrame({"id": [3, 4]})
    mock_read_sql = mocker.patch(
        "app.controller.extractor.pd.read_sql", return_value=[df1, df2]
    )
    mock_logger_info = mocker.patch("app.controller.extractor.logger.info")

    extractor = SQLServerExtractor(
        engine=mock_engine,
        columns_to_select=["id"],
        schema_name="dbo",
        table_name="users",
        cursor_column="id",
        chunk_size=2,
    )

    # Collect yielded chunks
    chunks = list(extractor.extract_chunks(last_cursor="2025-01-01 00:00:00.000"))

    # Assertions
    assert len(chunks) == 2
    assert chunks[0][0] == 1  # chunk index
    pd.testing.assert_frame_equal(chunks[0][1], df1)
    assert chunks[1][0] == 2
    pd.testing.assert_frame_equal(chunks[1][1], df2)

    # Verify pd.read_sql was called correctly
    mock_read_sql.assert_called_once()
    call_args = mock_read_sql.call_args
    assert call_args.kwargs["params"] == ("2025-01-01 00:00:00.000",)
    assert call_args.kwargs["chunksize"] == 2

    # Verify logging
    assert mock_logger_info.call_count == 4  # Start, chunk 1, chunk 2, finish


def test_extract_chunks_handles_read_sql_error(
    mock_engine: MagicMock, mocker: MockerFixture
):
    """
    Tests that an error during pd.read_sql is logged and re-raised.
    """
    mocker.patch(
        "app.controller.extractor.pd.read_sql",
        side_effect=ValueError("DB connection failed"),
    )
    mock_logger_error = mocker.patch("app.controller.extractor.logger.error")

    extractor = SQLServerExtractor(
        engine=mock_engine,
        columns_to_select=["id"],
        schema_name="dbo",
        table_name="users",
        cursor_column="id",
    )

    with pytest.raises(ValueError, match="DB connection failed"):
        # We need to consume the generator to trigger the code
        list(extractor.extract_chunks(last_cursor="0"))

    mock_logger_error.assert_called_once_with("Error reading SQL Query")
