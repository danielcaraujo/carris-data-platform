-- Macro to extract the latest partition_date from table
{% macro max_partition_date(source_name,ref_table) %}
(
    SELECT MAX(partition_date) AS partition_date
    FROM {{ source(source_name, ref_table) }}
)
{% endmacro %}