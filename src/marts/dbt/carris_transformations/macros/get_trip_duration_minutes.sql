-- Macro to calculate trip duration in minutes
{% macro get_trip_duration_minutes(start_seconds, end_seconds, output_column_name='duration_minutes') %}
    ROUND(({{ end_seconds }} - {{ start_seconds }}) / 60) AS {{ output_column_name }}
{% endmacro %}