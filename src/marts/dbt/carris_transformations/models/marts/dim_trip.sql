{% set surrogate_key_columns = [
    "trip_id"
] %}

with
    trips_staging as (
        select *
        from `data-eng-dev-437916.applied_project_staging_grupo_1.staging_trips`
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
            as trip_key,
            trip_id,
            direction_id as direction,
            route_id,
            service_id,
            trip_headsign,
            pattern_id,
            shape_id,
            current_timestamp as ingested_at
        from trips_staging
        where trip_id is not null
    )

select *
from final