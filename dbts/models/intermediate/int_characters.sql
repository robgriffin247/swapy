WITH characters AS (
    SELECT
        url AS character_id, 
        _dlt_id AS character_dlt_id,
        name AS character, 
        CASE WHEN gender = 'male' THEN 'Male' 
            WHEN gender = 'female' THEN 'Female' 
            WHEN gender = 'hermaphrodite' THEN 'Hermaphrodite' 
            ELSE NULL END AS sex,
        homeworld
        from {{ source('staging', 'stg_people') }}
)

SELECT * FROM characters