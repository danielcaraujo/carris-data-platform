/*Quais são as linhas que param em cada paragem?
Top 10 paragens com mais linhas*/
SELECT 
    stop_id,
    stop_name,
    municipality_name,
    COUNT(DISTINCT line_id) AS total_lines,
    STRING_AGG(DISTINCT line_short_name, ', ' ORDER BY line_short_name) AS lines
FROM {{ ref("fact_stop_event") }}
GROUP BY stop_id, stop_name, municipality_name
ORDER BY total_lines DESC
LIMIT 10;