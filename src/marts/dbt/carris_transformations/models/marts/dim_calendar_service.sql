{% set surrogate_key_columns = [
    "d.date",
    "ci.service_id"
] %}

with
    dates as (
        select *
        FROM {{ source ('carris_transformations','staging_dates') }}
    ),
    calendar_info as (
        select *
        FROM {{ source ('carris_transformations','staging_calendar_dates') }}
    ),
    final as (
        select distinct
            {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
            as calendar_service_key,
            d.date,
            ci.service_id,
            current_timestamp as ingested_at
        from dates d 
        join calendar_info ci
        on d.date = ci.date
    )

select *
from final
