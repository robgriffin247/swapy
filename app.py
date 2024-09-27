import streamlit as st
import duckdb
import plotly.express as px
import os

st.header("SWAPY")

with duckdb.connect(os.getenv("DUCKDB_PATH")) as con:

    # BACKEND ==============================================================================
    film_sex_counts = con.sql("""SELECT film, sex, COUNT(*) AS N
                                  FROM MAIN_CORE.DIM_FILM_CHARACTERS
                                  GROUP BY release_date, film, sex
                                  ORDER BY release_date, film""").to_df()

    plot_sex = px.bar(film_sex_counts,
                        x="film", y="N", color="sex",
                        labels={"film":"Film (oldest to newest)", "N":"Number of Characters", "sex":"Sex"})
    

                                  
    film_homeworld_counts = con.sql("""SELECT film, homeworld, COUNT(*) AS N
                                  FROM MAIN_CORE.DIM_FILM_CHARACTERS
                                  GROUP BY release_date, film, homeworld
                                  HAVING N>=2
                                  ORDER BY release_date, film""").to_df()

    plot_homeworld = px.bar(film_homeworld_counts,
                        x="film", y="N", color="homeworld",
                        labels={"film":"Film (oldest to newest)", "N":"Number of Characters", "homeworld":"Planet"})
    
    # FRONTEND =============================================================================
    st.subheader("How many characters per film, split by sex")
    st.plotly_chart(plot_sex)    
    
    st.subheader("How many characters per film, split by home planet (planets with >1 representants)")
    st.plotly_chart(plot_homeworld)
