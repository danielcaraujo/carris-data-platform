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
    pattern_id,
    facility_id,
    CASE WHEN school = 1 AND facility_id != 'school' then 0
         WHEN school = 0 AND facility_id = 'school' then 1
         WHEN school = 1 AND facility_id = 'school' then 1
    ELSE 0 END AS school,
    district_id,
    locality_id,
    municipality_id,
    region_id,
    replace(stop_id, 'NA', 'N/A') AS stop_id,
    _endpoint,
    _source_file,
    ingested_at
FROM {{ source('carris_transformations', 'staging_routes_exploded') }}