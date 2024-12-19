{% snapshot scd2_silver_dim_hosts %}

{{
   config(
       target_schema = 'DEV_scd',
       strategy='check',
       unique_key='HOST_ID_SPK',
       check_cols=['HOST_NAME', 'IS_SUPERHOST'],
       invalidated_hard_deletes=True
   )
}}

SELECT
    *
FROM
    {{ ref('silver_dim_hosts') }}

{% endsnapshot %}