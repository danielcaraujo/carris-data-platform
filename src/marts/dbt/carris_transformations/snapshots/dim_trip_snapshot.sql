{% snapshot dim_trip_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='trip_id',
      strategy='timestamp',
      updated_at='ingested_at',
    )
}}

SELECT 
    trip_id,
    direction,
    route_id,
    service_id,
    trip_headsign,
    pattern_id,
    shape_id,
    ingested_at
FROM {{ ref('dim_trip') }}

{% endsnapshot %}