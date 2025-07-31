WITH stops_by_region AS (
    SELECT 
        region_id,
        region_name,
        COUNT(DISTINCT stop_id) AS total_stops
    FROM {{ ref("fact_stop_event") }}
    GROUP BY region_id, region_name
)
SELECT 
    region_name,
    total_stops,
    ROUND(100.0 * total_stops / SUM(total_stops) OVER(), 2) AS percentage_of_total
FROM stops_by_region
ORDER BY total_stops DESC;