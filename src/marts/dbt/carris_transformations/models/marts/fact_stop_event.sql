{{ standardize_partitioning('event_date', 'date') }}

{% set surrogate_key_columns = [
    "stop_id",
    "line_id",
    "event_date"
] %}

WITH stop_times AS (
    SELECT 
        st.stop_id,
        st.trip_id,
        {{ convert_gtfs_time('st.arrival_time', 'arrival_time') }},
        {{ convert_gtfs_time('st.departure_time', 'departure_time') }},
        {{ convert_gtfs_time_to_seconds('st.arrival_time', 'arrival_seconds') }},
        {{ convert_gtfs_time_to_seconds('st.departure_time', 'departure_seconds') }}
        --({{ convert_gtfs_time_to_seconds('st.departure_time') }} - {{ convert_gtfs_time_to_seconds('st.arrival_time') }}) AS dwell_time_seconds
    FROM {{ ref("stg_stop_times") }} st
),

trips_with_routes AS (
    SELECT 
        t.trip_id,
        t.route_id,
        t.service_id,
        r.line_id
    FROM {{ ref("stg_trips") }} t
    LEFT JOIN {{ ref("stg_routes") }} r ON t.route_id = r.route_id
),

calendar_dates AS (
    SELECT 
        service_id,
        service_date,
        day_type,
        exception_type,
        holiday
    FROM {{ ref("stg_calendar_service") }}
),

stop_event_base AS (
    SELECT
        st.stop_id,
        twr.line_id,
        twr.route_id,
        cd.service_date AS event_date,
        cd.day_type,
        cd.exception_type,
        cd.holiday,
        COUNT(*) AS total_scheduled_arrivals,
        --AVG(st.dwell_time_seconds) AS avg_dwell_time_seconds,
        MIN(st.arrival_time) AS first_arrival,
        MAX(st.departure_time) AS last_departure,
        COUNT(CASE WHEN {{ determine_peak_period('st.arrival_time', 'cd.day_type', 'cd.exception_type', 'cd.holiday') }} THEN 1 END) AS peak_arrivals,
        COUNT(CASE WHEN NOT {{ determine_peak_period('st.arrival_time', 'cd.day_type', 'cd.exception_type', 'cd.holiday') }} THEN 1 END) AS off_peak_arrivals
    FROM stop_times st
    JOIN trips_with_routes twr ON st.trip_id = twr.trip_id
    JOIN calendar_dates cd ON twr.service_id = cd.service_id
    GROUP BY 1,2,3,4,5,6,7
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
                as stop_event_key,
        stop_id,
        line_id,
        route_id,
        event_date,
        total_scheduled_arrivals,
        --ROUND(avg_dwell_time_seconds, 2) AS avg_dwell_time_seconds,
        peak_arrivals,
        off_peak_arrivals,
        {{ determine_peak_period('first_arrival', 'day_type', 'exception_type', 'holiday') }} AS has_peak_service,
        current_timestamp() AS ingested_at
    FROM stop_event_base
),

SELECT *
FROM final