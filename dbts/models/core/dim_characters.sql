WITH characters AS (
    SELECT * FROM {{ ref("int_characters") }}
),
planets AS (
    SELECT planet_id, planet FROM {{ ref("int_planets") }}
)

SELECT 
    c.character_id, 
    c.character,
    c.height,
    c.mass,
    c.gender,
    p.planet as homeworld
FROM characters AS c LEFT JOIN planets AS p ON c.homeworld=p.planet_id