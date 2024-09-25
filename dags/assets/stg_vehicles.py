#from api_wan.api_wan import api_wan
from dagster import asset
import os 
import dlt
from dlt.sources.helpers import requests

@asset(group_name=f"{os.getenv('SCHEMA_STG')}")
def stg_vehicles() -> None:
    """Raw vehicle data from swapi api, json data stored as a table via a pandas dataframe"""
    url = f"https://swapi.dev/api/vehicles"
    content =  []

    while url != None:

        response = requests.get(url, timeout=90)
        response.raise_for_status()
        response_json = response.json()

        url = response_json["next"]
        content = content + [item for item in response_json["results"]]

    pipeline = dlt.pipeline(
        pipeline_name="swapi", # Issue - connect to existing data/swapi db
        destination="duckdb",
        dataset_name=f"{os.getenv('SCHEMA_STG')}"
    )

    pipeline.run(
        content,
        table_name="stg_vehicles",
        write_disposition="replace" 
    )
