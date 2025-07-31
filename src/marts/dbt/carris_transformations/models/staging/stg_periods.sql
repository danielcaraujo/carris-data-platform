WITH latest_partition AS {{ max_partition_date('carris_transformations','staging_municipalities') }}

SELECT p.*
FROM {{ source('carris_transformations', 'staging_periods') }} p
INNER JOIN latest_partition lp ON p.partition_date = lp.partition_date