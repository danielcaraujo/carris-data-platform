WITH latest_partition AS {{ max_partition_date('carris_transformations','staging_shapes') }}

SELECT s.*
FROM {{ source('carris_transformations', 'staging_shapes') }} s
INNER JOIN latest_partition lp ON s.partition_date = lp.partition_date