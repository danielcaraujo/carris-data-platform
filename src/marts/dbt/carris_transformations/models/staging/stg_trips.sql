WITH latest_partition AS {{ max_partition_date('carris_transformations','staging_stop_times') }}

SELECT 
    trip_id,
    direction_id,
    route_id,
    pattern_id,
    service_id,
    shape_id,
    replace(calendar_desc, 'NaN', 'N/A') AS calendar_desc,
    trip_headsign,
    _source_file,
    t.partition_date,
    ingested_at
FROM {{ source('carris_transformations','staging_trips') }} t
INNER JOIN latest_partition lp ON t.partition_date = lp.partition_date