WITH FILM_CHARACTERS AS (
    SELECT * FROM {{ source('staging', 'stg_films__characters') }}
),

FILMS AS (
    SELECT * FROM {{ ref('int_films') }}
),

CHARACTERS AS (
    SELECT * FROM {{ ref('int_characters') }}
),

PLANETS AS (
    SELECT * FROM {{ ref('int_planets') }}
)

SELECT F.FILM, F.EPISODE, F.RELEASE_DATE, C.CHARACTER, C.SEX, P.PLANET AS homeworld 
FROM FILM_CHARACTERS AS FC 
    LEFT JOIN CHARACTERS AS C ON FC.value=C.character_id
    LEFT JOIN PLANETS AS P ON C.homeworld=P.planet_id    
    LEFT JOIN FILMS AS F ON FC._dlt_parent_id=F.film_dlt_id