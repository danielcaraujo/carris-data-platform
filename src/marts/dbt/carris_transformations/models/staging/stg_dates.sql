SELECT 
    {{ parse_gtfs_date('d.date', 'day_date') }},
    d.date,
    d.day_type,
    d.holiday,
    replace(d.notes, 'NaN', 'N/A') AS notes,
    d.period,
    d._source_file,
    d.ingested_at
FROM {{ source ('carris_transformations','staging_dates') }} d