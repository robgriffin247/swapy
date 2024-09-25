import dlt
from dlt.sources.helpers import requests

url = f"https://swapi.dev/api/vehicles"
content =  []

while url != None:

    response = requests.get(url, timeout=90)
    response.raise_for_status()
    response_json = response.json()

    url = response_json["next"]
    content = content + [item for item in response_json["results"]]

pipeline = dlt.pipeline(
    pipeline_name="stg_vehicles",
    destination="duckdb",
    dataset_name="swapi.duckdb"
)

load_info = pipeline.run(
    content,
    table_name="stg_vehicles",
    write_disposition="replace",
)

print(load_info)
