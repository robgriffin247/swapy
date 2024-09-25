import streamlit as st
import duckdb
import plotly.express as px


st.header("SWAPY")

with duckdb.connect("data/swapi.duckdb") as con:
    # BACKEND ==============================================================================
    film_gender_counts = con.sql("""SELECT TITLE, GENDER, COUNT(*) AS N
                                  FROM MAIN_CORE.DIM_FILM_CHARACTERS
                                  WHERE GENDER NOT IN ('none', 'n/a')
                                  GROUP BY RELEASE_DATE, TITLE, GENDER
                                  ORDER BY RELEASE_DATE, TITLE""").to_df()

    plot_gender = px.bar(film_gender_counts,
                        x="title", y="N", color="gender",
                        labels={"title":"Film (oldest to newest)", "N":"Number of Characters", "gender":"Gender"})
    

                                  
    film_homeworld_counts = con.sql("""SELECT TITLE, HOMEWORLD, COUNT(*) AS N
                                  FROM MAIN_CORE.DIM_FILM_CHARACTERS
                                  GROUP BY RELEASE_DATE, TITLE, HOMEWORLD
                                  HAVING N>=2
                                  ORDER BY RELEASE_DATE, TITLE""").to_df()

    plot_homeworld = px.bar(film_homeworld_counts,
                        x="title", y="N", color="homeworld",
                        labels={"title":"Film (oldest to newest)", "N":"Number of Characters", "homeworld":"Planet"})
    
    # FRONTEND =============================================================================
    st.subheader("How many characters per film, split by gender")
    st.plotly_chart(plot_gender)    
    
    st.subheader("How many characters per film, split by home planet (planets with >1 representants)")
    st.plotly_chart(plot_homeworld)
