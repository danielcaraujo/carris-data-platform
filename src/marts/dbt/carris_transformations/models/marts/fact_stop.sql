{{ standardize_partitioning('event_date', 'date') }}

{% set surrogate_key_columns = [
    "stop_id",
    "trip_id",
    "line_id",
    "event_date"
] %}

WITH trips_with_routes AS (
    SELECT 
        t.trip_id,
        t.route_id,
        t.service_id,
        r.line_id,
        r.line_long_name,
        r.line_short_name, 
        r.facility_id,
        r.school
    FROM {{ ref("stg_trips") }} t
    LEFT JOIN {{ ref("stg_routes") }} r ON t.route_id = r.route_id
),

calendar_service AS (
    SELECT 
        service_id,
        service_date,
        d.date,
        day_type,
        exception_type,
        holiday
    FROM {{ ref("stg_calendar_service") }} d
),

trips_with_dates AS (
    SELECT
        t.*,
        cs.service_date,
        cs.day_type,
        cs.exception_type,
        cs.holiday
    FROM trips_with_routes t
    JOIN calendar_service cs ON t.service_id = cs.service_id
),

municipalities AS (
    SELECT *
    FROM {{ ref("stg_municipalities") }}
),

stop_times AS (
    SELECT 
        st.stop_id,
        st.trip_id,
        st.stop_sequence,
        st.shape_dist_traveled
    FROM {{ ref("stg_stop_times") }} st
),

stops AS (
    SELECT
        stp.*
    FROM {{ ref('stg_stops') }} stp
    LEFT JOIN municipalities m ON CAST(stp.municipality_id AS INT64) = m.municipality_id
)

SELECT * FROM stops