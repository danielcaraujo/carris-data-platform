-- Macro to convert GTFS times (which may be >24h) to valid TIME format
{% macro convert_gtfs_time(time_column, output_column_name='converted_time') %}
    TIME(
        MOD(CAST(SPLIT({{ time_column }}, ':')[OFFSET(0)] AS INT64), 24),
        CAST(SPLIT({{ time_column }}, ':')[OFFSET(1)] AS INT64),
        CAST(SPLIT({{ time_column }}, ':')[OFFSET(2)] AS INT64)
    ) AS {{ output_column_name }}
{% endmacro %}