{% set surrogate_key_columns = [
    "id"
] %}

with
    lines_staging as (
        select *
        from `data-eng-dev-437916.applied_project_staging_grupo_1.staging_lines`
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
            as line_key,
            id as line_id,
            'Carris Metropolitana' as operator,
            long_name as route_name,
            short_name,
            color,
            text_color,
            current_timestamp as ingested_at
        from lines_staging
        where id is not null
    )

select *
from final