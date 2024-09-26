/*WITH films AS (
    SELECT title, episode, release_date, unnest(characters) AS character_id FROM {{ref("int_films")}}
),
characters AS (
    SELECT * FROM {{ ref("dim_characters") }}
)

SELECT f.title, f.episode, f.release_date, c.character, c.height, c.mass, c.gender, c.homeworld
FROM films AS f LEFT JOIN characters AS c ON f.character_id=c.character_id*/

-- This needs a rebuild; take stg_films__characters, merge on film name, episode, release_date (by _dlt_id; add these to intermediates), and character data