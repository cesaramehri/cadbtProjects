{% snapshot scd_type2_raw_hosts %}

{{
   config(
       target_schema = 'DEV_scd',
       strategy='check',
       unique_key='ID',
       check_cols=['NAME', 'IS_SUPERHOST'],
       invalidated_hard_deletes=True
   )
}}

SELECT
    *
FROM
    {{ source('airbnb', 'RAW_HOSTS') }}

{% endsnapshot %}