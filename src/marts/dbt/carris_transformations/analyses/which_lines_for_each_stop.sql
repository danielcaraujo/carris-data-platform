SELECT 
    stop_id,
    stop_name,
    municipality_name,
    STRING_AGG(DISTINCT line_short_name, ', ' ORDER BY line_short_name) AS lines_serving_stop,
    COUNT(DISTINCT line_id) AS total_lines,
    STRING_AGG(DISTINCT 
        CONCAT(line_short_name, ' (', line_long_name, ')'), 
        '; ' ORDER BY line_short_name
    ) AS lines_with_details
FROM {{ ref("fact_stop_event") }}
GROUP BY stop_id, stop_name, municipality_name
ORDER BY total_lines DESC, stop_name;