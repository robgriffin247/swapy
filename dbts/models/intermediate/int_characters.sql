WITH characters AS (
    SELECT
        url as character_id,
        name as character,
        height,
        mass,
        CASE WHEN gender = 'male' THEN 'Male' 
            WHEN gender = 'female' THEN 'Female' 
            WHEN gender = 'hermaphrodite' THEN 'Hermaphrodite' 
            ELSE NULL END as gender,
        homeworld
    from {{ source('staging', 'stg_people') }}
)

SELECT * FROM characters