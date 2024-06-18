select
    title,
    episode_id as episode,
    opening_crawl,
    director,
    producer as producers,
    release_date as release_date_us,
    created as created_date,
    edited as edited_date
from {{ source('staging', 'stg_films') }}