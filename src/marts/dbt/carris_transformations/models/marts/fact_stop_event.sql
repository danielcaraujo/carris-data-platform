{{ standardize_partitioning('stop_event_date', 'date') }}

{% set surrogate_key_columns = [
    "st.stop_id",
    "st.trip_id",
    "st.stop_event_date",
    "st.stop_sequence"
] %}

WITH trips_with_routes AS (
    SELECT 
        t.trip_id,
        t.route_id,
        r.route_long_name,
        r.route_short_name,
        t.service_id,
        r.line_id,
        r.line_long_name,
        r.line_short_name, 
        r.school
    FROM {{ ref("stg_trips") }} t
    INNER JOIN {{ ref("stg_routes") }} r ON t.route_id = r.route_id
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
        cs.date,
        cs.day_type,
        cs.exception_type,
        cs.holiday
    FROM trips_with_routes t
    INNER JOIN calendar_service cs ON t.service_id = cs.service_id
),

stops_enriched AS (
    SELECT 
        s.stop_id,
        s.stop_name,
        s.municipality_id,
        s.municipality_name,
        s.region_id,
        s.region_name,
        m.district_id,
        m.district_name,
        s.near_school
    FROM {{ ref("dim_stop") }} s
    INNER JOIN {{ ref("stg_municipalities") }} m ON CAST(s.municipality_id AS INT64) = m.municipality_id
),

stop_times_with_trips AS (
    SELECT
        st.*,
        t.route_id,
        t.route_long_name,
        t.route_short_name,
        t.line_id,
        t.line_long_name,
        t.line_short_name, 
        t.service_id,
        t.school,
        t.service_date AS stop_event_date,
        t.date,
        t.day_type,
        t.exception_type,
        t.holiday
    FROM {{ ref("stg_stop_times") }} st
    INNER JOIN trips_with_dates t ON st.trip_id = t.trip_id
),

final AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} AS stop_event_key,
        st.stop_event_date,
        st.stop_id,
        s.stop_name,
        
        s.municipality_id,
        s.municipality_name,
        s.region_id,
        s.region_name,
        s.district_id,
        s.district_name,
        
        st.line_id,
        st.line_long_name,
        st.line_short_name,
        st.route_id,
        st.route_long_name,
        st.route_short_name,
        
        st.trip_id,
        st.stop_sequence,
        st.pickup_type,
        st.drop_off_type,
        
        CASE WHEN s.near_school = 1 THEN TRUE ELSE FALSE END AS is_near_school,
        CASE WHEN st.holiday = 1 THEN TRUE ELSE FALSE END AS is_holiday,
        CASE WHEN st.day_type IN (2,3) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN st.school = 1 THEN TRUE ELSE FALSE END AS serves_school_route,
                
        1 AS stop_event_count,

        current_timestamp() AS ingested_at
        
    FROM stop_times_with_trips st
    INNER JOIN stops_enriched s ON st.stop_id = s.stop_id
)

SELECT * FROM final