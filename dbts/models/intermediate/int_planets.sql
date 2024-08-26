select
    url as planet_id,
    name as planet,
    diameter,
    population
from {{ source('staging', 'stg_planets') }}