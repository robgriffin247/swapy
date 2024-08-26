import streamlit as st
import duckdb

st.header("SWAPY")

st.write("SWAPI data explorer built using Python, Dagster, dbt and Streamlit")


with duckdb.connect("data/swapi.duckdb") as con:
    
    df = con.sql("""SELECT *
                     FROM MAIN_CORE.DIM_FILM_PLANETS
                     """).to_df()
    
    summary = df.groupby(["Film"]).count().reset_index().rename(columns={"planet": "Number of Planets"})
    st.dataframe(summary, hide_index=True)

