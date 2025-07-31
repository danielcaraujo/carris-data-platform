WITH latest_partition AS {{ max_partition_date('carris_transformations','staging_municipalities') }}

SELECT m.*
FROM {{ source('carris_transformations', 'staging_municipalities') }} m
INNER JOIN latest_partition lp ON m.partition_date = lp.partition_date