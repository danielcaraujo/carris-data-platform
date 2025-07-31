{% set surrogate_key_columns = [
    "trip_id"
] %}

WITH
    trips_staging AS (
        SELECT *
        FROM {{ ref("stg_trips") }}
    ),

    final AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} AS trip_key,
            trip_id,
            direction_id AS direction,
            route_id,
            service_id,
            trip_headsign,
            pattern_id,
            shape_id,
            current_timestamp() AS ingested_at
        FROM trips_staging
    )

SELECT *
FROM final