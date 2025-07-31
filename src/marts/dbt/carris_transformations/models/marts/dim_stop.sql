{% set surrogate_key_columns = [
    "stop_id"
] %}

WITH stops_staging AS (
    SELECT 
        s.*,
        row_number() OVER (PARTITION BY stop_id ORDER BY ingested_at) AS row_num
    FROM {{ ref("stg_stops") }} s
),

final AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} AS stop_key,
        stop_id,
        stop_name,
        stop_latitude,
        stop_longitude,
        municipality_id,
        municipality_name,
        region_id,
        region_name,
        near_school,
        current_timestamp() AS ingested_at
    FROM stops_staging
    WHERE row_num = 1
)

SELECT *
FROM final
