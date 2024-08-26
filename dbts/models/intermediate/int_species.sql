select
    edited as edited_date
from {{ source('staging', 'stg_species') }}