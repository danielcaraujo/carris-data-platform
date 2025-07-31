SELECT 
    ROUND(AVG(duration_minutes), 2) AS avg_trip_duration_minutes,
    ROUND(MIN(duration_minutes), 2) AS min_duration_minutes,
    ROUND(MAX(duration_minutes), 2) AS max_duration_minutes,
    ROUND(STDDEV(duration_minutes), 2) AS stddev_duration_minutes,
    COUNT(*) AS total_trips_analyzed
FROM {{ ref('fact_trip_schedule') }}
WHERE duration_minutes > 0;