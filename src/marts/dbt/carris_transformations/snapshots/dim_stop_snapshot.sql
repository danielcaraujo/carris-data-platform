{% snapshot snapshot_dim_stop %}

{{
    config(
      target_schema='snapshots',
      unique_key='stop_id',
      strategy='check',
      check_cols=[
        'stop_name',
        'stop_latitude',
        'stop_longitude',
        'municipality_id',
        'municipality_name',
        'region_id',
        'region_name',
        'near_school'
      ]
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