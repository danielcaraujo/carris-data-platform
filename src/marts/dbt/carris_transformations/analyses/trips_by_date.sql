/*Qual é o número total de viagens agendadas por dia?*/
SELECT 
    trip_date,
    COUNT(*) AS total_trips_scheduled
FROM {{ ref('fact_trip_schedule') }}
GROUP BY trip_date
ORDER BY trip_date DESC;