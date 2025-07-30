{% set surrogate_key_columns = [
    "stop_id"
] %}

with
    stops_staging as (
        select *
        from `data-eng-dev-437916.applied_project_staging_grupo_1.staging_stops`
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
            as stop_key,
            stop_id,
            stop_name,
            stop_latitude as latitude,
            stop_longitude as longitude,
            municipality_name as municipality,
            region_name as region,
            near_school,
            current_timestamp as ingested_at
        from stops_staging
        where stop_id is not null
    )

select *
from final