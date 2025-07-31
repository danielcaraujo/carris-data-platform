-- Macro to determine whether a trip is during peak period
{% macro determine_peak_period(time_column, day_type_column, exception_type_column=none, holiday_column=none) %}
    CASE
        WHEN {{ day_type_column }} = 1 
        {% if exception_type_column %}
            AND {{ exception_type_column }} = 0
        {% endif %}
        {% if holiday_column %}
            AND {{ holiday_column }} = 0
        {% endif %}
        THEN
            CASE
                WHEN {{ time_column }} BETWEEN TIME '07:00:00' AND TIME '09:00:00'
                     OR {{ time_column }} BETWEEN TIME '17:00:00' AND TIME '20:00:00'
                THEN TRUE 
                ELSE FALSE
            END
        ELSE FALSE
    END
{% endmacro %}