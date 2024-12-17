{% snapshot scd_type2_raw_listings %}

{{
   config(
       target_schema='DEV_stg',
       unique_key='id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidated_hard_deletes=True
   )
}}

SELECT
    *
FROM
    {{ source('airbnb', 'RAW_LISTINGS') }}

{% endsnapshot %}