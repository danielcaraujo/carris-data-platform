{% snapshot snapshot_dim_line %}

{{
    config(
      target_schema='snapshots',
      unique_key='route_id',
      strategy='check',
      check_cols=[
        'route_long_name',
        'route_short_name',
        'route_type',
        'route_color',
        'route_text_color',
        'line_id',
        'line_long_name',
        'line_short_name',
        'line_type',
        'agency_id',
        'tts_name',
        'circular',
        'school'
      ]
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
