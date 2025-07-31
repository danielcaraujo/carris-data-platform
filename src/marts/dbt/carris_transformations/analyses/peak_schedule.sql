SELECT 
    is_peak,
    COUNT(*) AS trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total
FROM {{ ref('fact_trip_schedule') }}
GROUP BY is_peak
ORDER BY is_peak DESC;