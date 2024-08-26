select
    url as species_id,
    name as species,
    classification
from {{ source('staging', 'stg_species') }}