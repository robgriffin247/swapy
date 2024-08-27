WITH films AS (
    SELECT title, unnest(characters) AS character_id FROM {{ref("int_films") }}
),
characters AS (
    SELECT * FROM {{ ref("int_characters") }}
)

SELECT f.title, c.character, c.height, c.mass, c.gender, c.homeworld
FROM films AS f LEFT JOIN characters AS c ON f.character_id=c.character_id