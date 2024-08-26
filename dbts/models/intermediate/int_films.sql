select
    url as film_id,
    title,
    episode_id as episode,
    characters,
    planets, 
    species
from {{ source('staging', 'stg_films') }}