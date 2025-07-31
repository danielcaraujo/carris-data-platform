{{ standardize_partitioning('trip_date', 'date') }}

{% set surrogate_key_columns = [
    "t.trip_id",
    "cs.service_date"
] %}

WITH trips AS (
    SELECT t.* 
    FROM {{ ref("stg_trips") }} t
),

calendar_service AS (
    SELECT 
        service_id,
        service_date,
        day_type,
        exception_type,
        holiday
    FROM {{ ref("stg_calendar_service") }} cs
),

trips_with_dates AS (
    SELECT
        t.*,
        cs.service_date,
        cs.day_type,
        cs.exception_type,
        cs.holiday
    FROM trips t
    JOIN calendar_service cs ON t.service_id = cs.service_id
),

stop_times_converted AS (
    SELECT
        st.trip_id,
        st.stop_sequence,
        st.stop_id,
        {{ convert_gtfs_time_to_seconds('st.departure_time', 'departure_seconds') }},
        {{ convert_gtfs_time_to_seconds('st.arrival_time', 'arrival_seconds') }},
        {{ convert_gtfs_time('st.departure_time', 'departure_time') }},
        {{ convert_gtfs_time('st.arrival_time', 'arrival_time') }},
        st.shape_dist_traveled
    FROM {{ ref("stg_stop_times") }} st
),

trip_summary AS (
    SELECT
        trip_id,
        MIN(departure_time) AS departure_time_begin,
        MAX(arrival_time) AS arrival_time_end,
        {{ get_trip_duration_minutes('MIN(departure_seconds)', 'MAX(arrival_seconds)', 'duration_minutes') }},
        ROUND(MAX(shape_dist_traveled),2) AS distance_km,
        COUNT(*) AS total_stops
    FROM stop_times_converted
    GROUP BY trip_id
),

trip_peak_analysis AS (
    SELECT
        tc.trip_id,
        twd.service_date,
        MIN(tc.departure_time) AS first_departure,
        twd.day_type,
        twd.exception_type,
        twd.holiday,
        {{ determine_peak_period('MIN(tc.departure_time)', 'twd.day_type', 'twd.exception_type', 'twd.holiday') }} AS is_peak
    FROM stop_times_converted tc
    JOIN trips_with_dates twd ON tc.trip_id = twd.trip_id
    GROUP BY tc.trip_id, twd.service_date, twd.day_type, twd.exception_type, twd.holiday
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
            AS trip_key,
        t.trip_id,
        cs.service_date AS trip_date,
        t.route_id,
        t.pattern_id,
        t.shape_id,
        t.service_id,
        t.direction_id,
        t.trip_headsign,
        ts.departure_time_begin AS departure_time,
        ts.arrival_time_end AS arrival_time,
        ts.duration_minutes,
        COALESCE(tpa.is_peak, FALSE) AS is_peak,
        COALESCE(ts.distance_km, 0) AS distance_km,
        ts.total_stops,
        current_timestamp() AS ingested_at        
    FROM trips t
    JOIN calendar_service cs ON t.service_id = cs.service_id
    LEFT JOIN trip_summary ts ON t.trip_id = ts.trip_id
    LEFT JOIN trip_peak_analysis tpa ON tpa.trip_id = t.trip_id AND tpa.service_date = cs.service_date
)

SELECT *
FROM final