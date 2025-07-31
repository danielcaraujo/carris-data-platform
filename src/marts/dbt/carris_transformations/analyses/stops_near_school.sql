/*Quais são as paragens perto de uma escola?*/
SELECT 
    stop_id,
    stop_name,
    municipality_name,
    district_name,
    region_name,
    is_near_school,
    COUNT(DISTINCT line_id) AS total_lines_serving,
    STRING_AGG(DISTINCT line_short_name, ', ' ORDER BY line_short_name) AS lines_serving,
    COUNT(*) AS total_scheduled_arrivals,
    COUNT(DISTINCT stop_event_date) AS days_with_service
FROM {{ ref("fact_stop_event") }}
WHERE is_near_school = TRUE
GROUP BY stop_id, stop_name, municipality_name, 
         district_name, region_name, is_near_school
ORDER BY total_lines_serving DESC, stop_name;