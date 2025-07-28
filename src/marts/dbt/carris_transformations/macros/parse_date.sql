-- Macro to convert dates (YYYYMMDD) to DATE type
{% macro parse_gtfs_date(date_column, output_column_name='parsed_date') %}
    PARSE_DATE('%Y%m%d', CAST({{ date_column }} AS STRING)) AS {{ output_column_name }}
{% endmacro %}