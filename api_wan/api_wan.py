# api_wan(): load raw swapi data to duckdb
import os
import dlt
from dlt.sources.helpers import requests
import duckdb

def api_wan(resource, drop=[]):
    url = f"https://swapi.dev/api/{resource}"
    content = [] 

    while url != None:

        print(f"Getting {url}")
        response = requests.get(url, timeout=90)
        response.raise_for_status()
        response_json = response.json()

        url = response_json["next"]
        content = content + [item for item in response_json["results"]]


    for d in drop:
        for c in content:
            c.pop(d, None)


    pipeline = dlt.pipeline(
        pipeline_name="swapi",
        destination="duckdb",
        dataset_name=f"{os.getenv('SCHEMA_STG')}"
    )

    pipeline.run(
        content,
        table_name=f"stg_{resource}",
        write_disposition="replace"
    )

    with duckdb.connect(f"{os.getenv('DUCKDB_PATH')}") as con:
        con.sql(f"select * from {os.getenv('SCHEMA_STG')}.stg_{resource}").show()