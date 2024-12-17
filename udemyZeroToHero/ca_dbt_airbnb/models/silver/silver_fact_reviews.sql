-- Fact Tables Incremental Load
{{
  config(
    materialized = 'incremental',
    on_schema_change = 'fail'
    )
}}

WITH cte_bronze_raw_reviews AS
(
    SELECT 
        *
    FROM
        {{ ref('bronze_raw_reviews') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['LISTING_ID', 'REVIEW_DATE', 'REVIEWER_NAME']) }}, -- generate unique hashed SPK
    *
FROM
    cte_bronze_raw_reviews
WHERE
    REVIEW_TEXT IS NOT NULL
-- Define how to implement incremental load
{% if is_incremental() %}
  AND REVIEW_DATE > ( SELECT MAX(REVIEW_DATE) FROM {{this}} ) --this = actual model
{% endif %}

-- In case you want to rebuild the whole table from scratch => dbt run --full-refresh