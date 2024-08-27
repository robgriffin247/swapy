import streamlit as st
import duckdb

st.header("SWAPY")

st.write("SWAPI data explorer built using Python, Dagster, dbt and Streamlit")


with duckdb.connect("data/swapi.duckdb") as con:
    
    int_films = con.sql("""SELECT * FROM MAIN_INTERMEDIATE.INT_FILMS""").to_df()
    int_characters = con.sql("""SELECT * FROM MAIN_INTERMEDIATE.INT_CHARACTERS""").to_df()
    int_planets = con.sql("""SELECT * FROM MAIN_INTERMEDIATE.INT_PLANETS""").to_df()
    int_species = con.sql("""SELECT * FROM MAIN_INTERMEDIATE.INT_SPECIES""").to_df()

    dim_films = con.sql("""SELECT * FROM MAIN_CORE.DIM_FILMS""").to_df()
    dim_characters = con.sql("""SELECT * FROM MAIN_CORE.DIM_CHARACTERS""").to_df()

    st.dataframe(int_films)
    st.dataframe(int_characters)
    st.dataframe(int_planets)
    st.dataframe(int_species)
    st.dataframe(dim_films)
    st.dataframe(dim_characters)
    
    