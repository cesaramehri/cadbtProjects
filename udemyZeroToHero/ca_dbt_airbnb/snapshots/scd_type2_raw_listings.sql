{% snapshot scd_type2_raw_listings %}

{{
   config(
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