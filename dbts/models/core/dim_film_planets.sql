with source as (
    select 
        title as Film, 
        unnest(planets) as Planet
    from {{ ref('int_films')}}
),

joint as (
    select f.film, p.planet as Planet
    from source as f left join {{ ref('int_planets') }} as p on f.planet=p.planet_id
)

select * from joint