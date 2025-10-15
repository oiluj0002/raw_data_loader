from sqlalchemy import create_engine, inspect
from config.env import get_env_var

import pandas as pd

USER = get_env_var("USERNAME")
PASSWORD = get_env_var("PASSWORD")
HOST = get_env_var("HOSTNAME")
PORT = get_env_var("PORT")
DBNAME = get_env_var("DBNAME")

url = f"postgresql+psycopg://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
query = "SELECT * FROM customers"

engine = create_engine(url)
inspector = inspect(engine)

#with engine.connect() as conn:
#    df = pd.read_sql(query, engine, chunksize=100)

columns = {}
for col in inspector.get_columns("customers"):
    columns[col["name"]] = str(col["type"])

print(columns)
