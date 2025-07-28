{% set surrogate_key_columns = [
    "d.date",
    "ci.service_id",
    "pi.period_id"
] %}

WITH
    dates AS (
        SELECT *
        FROM {{ ref("stg_dates") }}
    ),
    calendar_info AS (
        SELECT *
        FROM {{ ref("stg_calendar_service") }}
    ),
    period_info AS (
        SELECT *
        FROM {{ ref("stg_periods") }}
    ),
    final AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} AS calendar_service_key,
            ci.service_date,
            d.date,
            ci.service_id,
            pi.period_id,
            pi.period_name,
            current_timestamp AS ingested_at
        FROM dates d 
        JOIN calendar_info ci
        ON d.date = ci.date
        JOIN period_info pi
        ON d.period = pi.period_id
    )

SELECT *
FROM final
