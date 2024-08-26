select 
    title as Film, 
    episode as Episode 
from {{ ref('int_films')}}