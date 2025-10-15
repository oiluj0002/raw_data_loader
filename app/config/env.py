import os
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv("secret/.env")
logger = get_logger()

@staticmethod
def get_env_var(var_name: str) -> str:
    """
        Collects environment variables and checks if they exists.

        Args:
            var_name (str): Environment variable name.
        
        Returns:
            value (str): Environment variable value.

    """
    value = os.getenv(var_name)
    if not value:
        logger.error(f"Environment variable not found: {var_name}.")
        raise EnvironmentError(f"Environment variable {var_name} not defined.")
    return value
