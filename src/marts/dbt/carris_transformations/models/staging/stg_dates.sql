WITH latest_partition AS {{ max_partition_date('carris_transformations','staging_dates') }}

SELECT 
    {{ parse_gtfs_date('d.date', 'day_date') }},
    d.date,
    d.day_type,
    d.holiday,
    replace(d.notes, 'NaN', 'N/A') AS notes,
    d.period,
    d._source_file,
    d.partition_date,
    d.ingested_at
FROM {{ source ('carris_transformations','staging_dates') }} d
INNER JOIN latest_partition lp ON d.partition_date = lp.partition_date