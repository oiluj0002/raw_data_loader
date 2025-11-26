from sqlalchemy import create_engine, Engine, URL, NullPool

from config import env
from utils.logger import get_logger

logger = get_logger()


def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy Engine instance for SQL Server.

    Uses the "mssql+pyodbc" dialect and reads connection details from
    environment variables via the config module.

    Returns:
    An authenticated SQLAlchemy Engine instance connected to SQL Server.

    Raises:
        Exception: If the database connection fails.
    """
    try:
        # Build connection URL using provided environment variables
        connection_url = URL.create(
            "mssql+pyodbc",
            username=env.DB_USER,
            password=env.DB_PASSWORD,
            host=env.DB_HOST,
            port=env.DB_PORT,
            database=env.DB_NAME,
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "TrustServerCertificate": "yes",
            },
        )

        engine = create_engine(connection_url, poolclass=NullPool)
        logger.info("Database engine created successfully.")
        return engine

    except Exception as e:
        logger.error(f"Failed to create database engine. Error: {e}")
        raise
