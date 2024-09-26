select
    url as film_id,
    title,
    episode_id as episode,
    release_date--,
    --characters,
    --planets, 
    --species
from {{ source('staging', 'stg_films') }}