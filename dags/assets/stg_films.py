from api_wan.api_wan import api_wan
from dagster import asset
import os 

@asset(group_name=f"{os.getenv('SCHEMA_STG')}")
def stg_films() -> None:
    """Raw films data from swapi api, json data stored as a table via a pandas dataframe"""
    api_wan("films", drop=["planets", "starships", "vehicles", "species"])