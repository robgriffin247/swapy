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

    return content

films = api_wan("films", ["planets", "starships", "vehicles", "species"])

print(films[0].keys())
"""
drop = ["planets", "starships", "vehicles", "species"]
for d in drop:
    films[0].pop(d, None)
print(films[0].keys())
"""