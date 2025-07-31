/*Qual é a distância total percorrida durante cada viagem?*/
SELECT 
    trip_id,
    trip_date,
    route_id,
    distance_km AS total_distance_km,
    duration_minutes
FROM {{ ref('fact_trip_schedule') }}
WHERE distance_km > 0
ORDER BY distance_km DESC;