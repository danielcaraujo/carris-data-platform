WITH stops_by_municipality AS (
    SELECT 
        municipality_id,
        municipality_name,
        district_name,
        region_name,
        COUNT(DISTINCT stop_id) AS total_stops
    FROM {{ ref("fact_stop_event") }}
    GROUP BY municipality_id, municipality_name, district_name, region_name
)
SELECT 
    municipality_name,
    district_name,
    region_name,
    total_stops,
    ROUND(100.0 * total_stops / SUM(total_stops) OVER(), 2) AS percentage_of_total
FROM stops_by_municipality
ORDER BY total_stops DESC;