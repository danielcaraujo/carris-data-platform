{% set surrogate_key_columns = [
    "route_id"
] %}

WITH routes_and_lines AS (
    SELECT
        line_id,
        line_long_name,
        line_short_name,
        line_type,
        route_id,
        route_long_name,
        route_short_name,
        route_type,
        route_color,
        route_text_color,
        agency_id,
        tts_name,
        circular,
        school
    FROM {{ ref('stg_routes') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} AS route_key,
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
        agency_id,
        tts_name,
        circular,
        school,
        current_timestamp() AS ingested_at
    FROM routes_and_lines
)
SELECT *
FROM final