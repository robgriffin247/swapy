from api_wan.api_wan import api_wan
import duckdb
import os

api_wan("films")

with duckdb.connect(f"{os.getenv('DUCKDB_PATH')}") as c:
    c.sql("show all tables").show()