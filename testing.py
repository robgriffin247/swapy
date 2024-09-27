import duckdb
import os

with duckdb.connect(os.getenv("DUCKDB_PATH")) as con:

    data = con.sql("""SELECT *
                        FROM MAIN_CORE.DIM_FILM_CHARACTERS""").to_df()
    
    print(data.keys())
    print(data)
