SELECT
    url AS planet_id,
    _dlt_id AS planet_dlt_id,
    name AS planet
FROM {{ source('staging', 'stg_planets') }}