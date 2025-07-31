/*Quantas vezes uma dada linha (line_id = '1111', de exemplo) passa numa paragem?*/
SELECT 
    stop_id,
    stop_name,
    municipality_name,
    COUNT(*) AS total_frequency,
    COUNT(DISTINCT stop_event_date) AS days_served,
    COUNT(*) / COUNT(DISTINCT stop_event_date) AS avg_frequency_per_day,
    COUNT(DISTINCT trip_id) AS unique_trips,
    MIN(stop_event_date) AS first_service_date,
    MAX(stop_event_date) AS last_service_date
FROM {{ ref("fact_stop_event") }}
WHERE line_id = '1111'
GROUP BY stop_id, stop_name, municipality_name
ORDER BY total_frequency DESC;