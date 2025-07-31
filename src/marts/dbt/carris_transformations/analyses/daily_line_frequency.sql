/*Quantas vezes uma dada linha passa numa paragem?*/
WITH daily_line_frequency AS (
    SELECT 
        stop_id,
        stop_name,
        line_id,
        line_short_name,
        line_long_name,
        stop_event_date,
        COUNT(*) AS daily_frequency,
        COUNT(DISTINCT trip_id) AS unique_trips_per_day
    FROM {{ ref("fact_stop_event") }}
    GROUP BY stop_id, stop_name, line_id, line_short_name, line_long_name, stop_event_date
)
SELECT 
    stop_name,
    line_short_name,
    line_long_name,
    ROUND(AVG(daily_frequency),0) AS avg_daily_frequency,
    MIN(daily_frequency) AS min_daily_frequency,
    MAX(daily_frequency) AS max_daily_frequency,
    SUM(daily_frequency) AS total_frequency,
    COUNT(DISTINCT stop_event_date) AS days_operated
FROM daily_line_frequency
GROUP BY stop_id, stop_name, line_id, line_short_name, line_long_name
ORDER BY avg_daily_frequency DESC;