select
    *
from {{ source('staging', 'stg_people') }}