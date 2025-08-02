{% snapshot snapshot_dim_trip %}

{{
    config(
      target_schema='snapshots',
      unique_key='trip_id',
      strategy='check',
      check_cols=[
        'direction',
        'route_id',
        'service_id',
        'trip_headsign',
        'pattern_id',
        'shape_id'
      ]
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
