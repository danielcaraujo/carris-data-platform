WITH latest_partition AS {{ max_partition_date('carris_transformations','staging_calendar_dates') }}

SELECT
    {{ parse_gtfs_date('date', 'service_date') }},
    s.*
FROM {{ source('carris_transformations', 'staging_calendar_dates') }} s
INNER JOIN latest_partition lp ON s.partition_date = lp.partition_date