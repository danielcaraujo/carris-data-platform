{% set surrogate_key_columns = [
    "st.stop_id",
    "st.trip_id", 
    "CAST(st.ingested_at AS DATE)"
] %}

with
    stop_times as (
        select *
        from `data-eng-dev-437916.applied_project_staging_grupo_1.staging_stop_times`
    ),

    trips as (
        select *
        from `data-eng-dev-437916.applied_project_staging_grupo_1.staging_trips`
    ),

    routes as (
        select *
        from `data-eng-dev-437916.applied_project_staging_grupo_1.staging_routes`
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
            as stop_event_key,
            st.stop_id,
            r.line_id,
            t.route_id,
            cast(extract(date from st.ingested_at) as string) as date_id,
            st.trip_id,
            st.arrival_time,
            st.departure_time,
            st.stop_sequence,
            st.timepoint,
            current_timestamp as ingested_at
        from stop_times st
        left join trips t
            on st.trip_id = t.trip_id
        left join routes r
            on t.route_id = r.route_id
        where st.stop_id is not null
          and st.trip_id is not null
    )

select *
from final