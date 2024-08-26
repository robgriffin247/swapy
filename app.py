import streamlit as st
import pandas as pd
import duckdb

st.header("SWAPY")

st.write("SWAPI data explorer built using Python, Dagster, dbt and Streamlit")


with duckdb.connect("data/swapi.duckdb") as con:
    
    st.dataframe(con.sql("""
                     SELECT *
                     FROM STAGING.STG_FILMS
                     """).to_df(),
                     hide_index=True)
    
    st.dataframe(con.sql("""
                     SELECT *
                     FROM INT_FILMS
                     """).to_df(),
                     hide_index=True)
    
    st.dataframe(con.sql("""
                     SELECT *
                     FROM MAIN_CORE.DIM_FILMS
                     """).to_df(),
                     hide_index=True)