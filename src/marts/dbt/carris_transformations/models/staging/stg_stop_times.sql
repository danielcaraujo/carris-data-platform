SELECT *
FROM {{ source('carris_transformations', 'staging_stop_times') }}