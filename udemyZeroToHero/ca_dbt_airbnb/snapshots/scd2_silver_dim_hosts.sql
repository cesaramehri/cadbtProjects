{% snapshot scd2_silver_dim_hosts %}

{%- set trgt_schema = target.schema + '_scd' -%}

{{
   config(
       target_schema = trgt_schema,
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