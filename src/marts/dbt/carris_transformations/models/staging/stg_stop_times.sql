WITH latest_partition AS {{ max_partition_date('carris_transformations','staging_stop_times') }}

SELECT s.*
FROM {{ source('carris_transformations', 'staging_stop_times') }} s
INNER JOIN latest_partition lp ON s.partition_date = lp.partition_date