select
    character, height, mass, gender, 
    planet as home_planet
from {{ ref('int_characters') }} as c left join {{ ref('int_planets') }} as p on c.home_planet=p.planet_id