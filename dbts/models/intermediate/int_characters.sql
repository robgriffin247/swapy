WITH characters AS (
    SELECT
        url as character_id,
        name as character,
        height,
        mass,
        gender,
        homeworld
    from {{ source('staging', 'stg_people') }}
)

SELECT * FROM characters