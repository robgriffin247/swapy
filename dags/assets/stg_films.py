from api_wan.api_wan import api_wan
from dagster import asset
import os 
import duckdb

@asset(group_name=f"{os.getenv('SCHEMA_STG')}")
def stg_films() -> None:
    """Raw films data from swapi api, json data stored as a table via a pandas dataframe"""
    api_wan("films", drop=["planets", "starships", "vehicles", "species"])

@asset(group_name=f"{os.getenv('SCHEMA_STG')}")
def stg_films__characters() -> None:
    """Raw film character appearances data from swapi api, json data stored as a table via a pandas dataframe"""
    with duckdb.connect(os.getenv("DUCKDB_PATH")) as con:
        print(con.sql("""SELECT * FROM STAGING.STG_FILMS__CHARACTERS"""))