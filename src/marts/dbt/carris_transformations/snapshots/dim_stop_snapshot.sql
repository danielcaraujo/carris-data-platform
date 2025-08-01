{% snapshot dim_stop_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='stop_id',
      strategy='timestamp',
      updated_at='ingested_at',
    )
}}

SELECT 
    stop_id,
    stop_name,
    stop_latitude,
    stop_longitude,
    municipality_id,
    municipality_name,
    region_id,
    region_name,
    near_school,
    ingested_at
FROM {{ ref('dim_stop') }}

{% endsnapshot %}