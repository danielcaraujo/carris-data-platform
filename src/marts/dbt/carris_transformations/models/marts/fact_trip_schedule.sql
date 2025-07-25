{% set surrogate_key_columns = [
    "t.trip_id"
] %}

with
    trips as (
        select *
        FROM {{ source ('carris_transformations','staging_trips') }}
    ),

    calendar_service as (
        select *
        FROM {{ source ('carris_transformations','staging_calendar_dates') }}
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
            as trip_key,
            cs.date AS trip_date,
            t.trip_id,
            t.route_id,
            t.pattern_id,
            t.shape_id,
            t.service_id,
            t.direction_id,           
            t.trip_headsign,
            current_timestamp as ingested_at
        from trips t
        left join calendar_service cs
        on t.service_id = cs.service_id
    )

select *
from final
