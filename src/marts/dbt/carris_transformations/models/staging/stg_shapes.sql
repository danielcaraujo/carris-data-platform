SELECT *
FROM {{ source('carris_transformations', 'staging_shapes') }}