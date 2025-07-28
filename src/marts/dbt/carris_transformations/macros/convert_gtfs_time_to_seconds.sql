-- Macro to convert GTFS times to total seconds
{% macro convert_gtfs_time_to_seconds(time_column, output_column_name='time_seconds') %}
    CAST(SPLIT({{ time_column }}, ':')[OFFSET(0)] AS INT64) * 3600 +
    CAST(SPLIT({{ time_column }}, ':')[OFFSET(1)] AS INT64) * 60 +
    CAST(SPLIT({{ time_column }}, ':')[OFFSET(2)] AS INT64) AS {{ output_column_name }}
{% endmacro %}