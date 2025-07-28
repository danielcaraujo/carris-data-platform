SELECT
    {{ parse_gtfs_date('date', 'service_date') }},
    *
FROM {{ source('carris_transformations', 'staging_calendar_dates') }}