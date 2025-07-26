{{ config(
    partition_by={
      "field": "trip_date",
      "data_type": "date"
    }
) }}

{% set surrogate_key_columns = [
    "t.trip_id",
    "cs.date"
] %}

WITH trips AS (
    SELECT * 
    FROM {{ source ('carris_transformations','staging_trips') }}
),

calendar_service AS (
    SELECT 
        service_id,
        PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS date,
        day_type,
        exception_type,
        holiday
    FROM {{ source ('carris_transformations','staging_calendar_dates') }}
),

-- Controlado: gera combinação de trip com as datas do serviço
trips_with_dates AS (
    SELECT
        t.*,
        cs.date,
        cs.day_type,
        cs.exception_type,
        cs.holiday
    FROM trips t
    JOIN calendar_service cs ON t.service_id = cs.service_id
),

tempos_convertidos AS (
    SELECT
        st.trip_id,
        twd.date AS trip_date,
        
        CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64)*3600 +
        CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64)*60 +
        CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64) AS departure_seconds_total,

        CAST(SPLIT(st.arrival_time, ':')[OFFSET(0)] AS INT64)*3600 +
        CAST(SPLIT(st.arrival_time, ':')[OFFSET(1)] AS INT64)*60 +
        CAST(SPLIT(st.arrival_time, ':')[OFFSET(2)] AS INT64) AS arrival_seconds_total,

        TIME(
            MOD(CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64), 24),
            CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64),
            CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64)
        ) AS departure_time,

        TIME(
            MOD(CAST(SPLIT(st.arrival_time, ':')[OFFSET(0)] AS INT64), 24),
            CAST(SPLIT(st.arrival_time, ':')[OFFSET(1)] AS INT64),
            CAST(SPLIT(st.arrival_time, ':')[OFFSET(2)] AS INT64)
        ) AS arrival_time,

        st.stop_sequence
    FROM {{ source ('carris_transformations', 'staging_stop_times') }} st
    JOIN trips_with_dates twd ON st.trip_id = twd.trip_id
),

inicio_fim AS (
    SELECT
        trip_id,
        MIN(departure_time) AS departure_time_inicio,
        MAX(arrival_time) AS arrival_time_fim
    FROM tempos_convertidos
    GROUP BY trip_id
),

duracao AS (
    SELECT
        trip_id,
        ROUND((MAX(arrival_seconds_total) - MIN(departure_seconds_total)) / 60) AS duracao_minutos
    FROM tempos_convertidos
    GROUP BY trip_id
),

distancia AS (
    SELECT 
        trip_id,
        shape_dist_traveled AS distancia_km
    FROM (
        SELECT 
            trip_id,
            shape_dist_traveled,
            ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence DESC) AS rn
        FROM {{ source ('carris_transformations', 'staging_stop_times') }}
    )
    WHERE rn = 1
),

peak_flag AS (
    SELECT
        tc.trip_id,
        twd.date,
        MIN(tc.departure_time) AS departure_time_inicio,
        twd.day_type,
        twd.exception_type,
        twd.holiday,
        CASE
            WHEN twd.day_type = 1 AND twd.exception_type = 0 AND twd.holiday = 0 THEN
                CASE
                    WHEN MIN(tc.departure_time) BETWEEN TIME '07:00:00' AND TIME '09:00:00'
                         OR MIN(tc.departure_time) BETWEEN TIME '17:00:00' AND TIME '20:00:00'
                    THEN TRUE ELSE FALSE
                END
            ELSE FALSE
        END AS is_peak
    FROM tempos_convertidos tc
    JOIN trips_with_dates twd ON tc.trip_id = twd.trip_id
    GROUP BY tc.trip_id, twd.date, twd.day_type, twd.exception_type, twd.holiday
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
            as trip_key,
        t.trip_id,
        cs.date AS trip_date,
        t.route_id,
        t.pattern_id,
        t.shape_id,
        t.service_id,
        t.direction_id,
        t.trip_headsign,
        i.departure_time_inicio AS departure_time,
        i.arrival_time_fim AS arrival_time,
        p.is_peak,
        d.distancia_km AS distance_km,
        du.duracao_minutos AS duration_minutes,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM trips t
    JOIN calendar_service cs ON t.service_id = cs.service_id
    LEFT JOIN distancia d ON t.trip_id = d.trip_id
    LEFT JOIN duracao du ON t.trip_id = du.trip_id
    LEFT JOIN peak_flag p ON p.trip_id = t.trip_id AND p.date = cs.date
    LEFT JOIN inicio_fim i ON t.trip_id = i.trip_id
)

SELECT *
FROM final