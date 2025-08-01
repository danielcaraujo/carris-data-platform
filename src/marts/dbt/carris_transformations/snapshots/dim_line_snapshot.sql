{% snapshot dim_line_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='route_id',
      strategy='timestamp',
      updated_at='ingested_at',
    )
}}

SELECT 
    route_id,
    route_long_name,
    route_short_name,
    route_type,
    route_color,
    route_text_color,
    line_id,
    line_long_name,
    line_short_name,
    line_type,
    agency_id,
    tts_name,
    circular,
    school,
    ingested_at
FROM {{ ref('dim_line') }}

{% endsnapshot %}