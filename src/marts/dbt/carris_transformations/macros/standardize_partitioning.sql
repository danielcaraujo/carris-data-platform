-- Macro to standardize table partition configuration
{% macro standardize_partitioning(partition_field, partition_type='date') %}
    {{ config(
        partition_by={
            "field": "{{ partition_field }}",
            "data_type": "{{ partition_type }}"
        }
    ) }}
{% endmacro %}