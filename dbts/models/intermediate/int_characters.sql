select
    url as character_id,
    name as character,
    height,
    mass,
    gender,
    homeworld as home_planet
from {{ source('staging', 'stg_people') }}