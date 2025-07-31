WITH latest_partition AS {{ max_partition_date('carris_transformations','staging_routes_converted') }}

SELECT 
    route_id,
    route_long_name,
    route_short_name,
    route_type,
    route_color,
    route_text_color,
    line_id,
    line_long_name,
    line_short_name,
    line_type,
    tts_name,
    agency_id,
    circular,
    path_type,
    school,
    district_ids,
    facilities,
    locality_ids,
    municipality_ids,
    pattern_ids,
    region_ids,
    stop_ids,
    _endpoint,
    _source_file,
    r.partition_date,
    ingested_at
FROM {{ source('carris_transformations', 'staging_routes_converted') }} r
INNER JOIN latest_partition lp ON r.partition_date = lp.partition_date