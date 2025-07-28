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
    ingested_at
FROM {{ source('carris_transformations','staging_trips') }}