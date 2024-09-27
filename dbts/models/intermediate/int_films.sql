SELECT  
    url AS film_id,
    _dlt_id AS film_dlt_id,
    title AS film,
    episode_id AS episode,
    release_date
FROM {{ source('staging', 'stg_films') }}