from sqlalchemy import create_engine, Engine

from config import env
from utils.logger import get_logger

logger = get_logger()


def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy Engine instance for the database.

    It reads connection details from the config module.

    Returns:
        An authenticated SQLAlchemy Engine instance.

    Raises:
        Exception: If the database connection fails.
    """
    try:
        # Nota: Connection string para PostgreSQL, como no nome da sua classe Extractor.
        conn_str = (
            f"postgresql+psycopg://{env.DB_USER}:{env.DB_PASSWORD}@"
            f"{env.DB_HOST}/{env.DB_NAME}"
        )

        # Para SQL Server, seria:
        # conn_str = (
        #     f"mssql+pyodbc://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}/"
        #     f"{config.DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
        # )

        engine = create_engine(conn_str)
        logger.info("Database engine created successfully.")
        return engine

    except Exception as e:
        logger.error(f"Failed to create database engine. Error: {e}")
        raise
