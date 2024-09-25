# api_wan(): load raw swapi data to duckdb
import httpx 
import pandas as pd
import os
import duckdb

def api_wan(resource):
    url = f"https://swapi.dev/api/{resource}"
    content = [] 

    while url != None:

        print(f"Getting {url}")
        response = httpx.get(url, timeout=90)
        response.raise_for_status()
        response_json = response.json()

        url = response_json["next"]
        content = content + [item for item in response_json["results"]]

    df = pd.DataFrame.from_dict(content)

    query = f"""
            create or replace table {os.getenv('SCHEMA_STG')}.stg_{resource} as (
                select
                    *
                from df
            );
        """
    with duckdb.connect(f"{os.getenv('DUCKDB_PATH')}") as con:
        con.sql(f"create schema if not exists {os.getenv('SCHEMA_STG')};")
        con.sql(query)
        con.sql(f"select * from {os.getenv('SCHEMA_STG')}.stg_{resource}").show()
