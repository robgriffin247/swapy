WITH films AS (
    SELECT * 
    FROM {{ ref("int_films")}}
)

SELECT film_id, title, episode, release_date
FROM films