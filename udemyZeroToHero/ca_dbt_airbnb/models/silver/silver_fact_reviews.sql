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
    {{ dbt_utils.generate_surrogate_key(['LISTING_ID', 'REVIEW_DATE', 'REVIEWER_NAME']) }} AS REVIEW_ID_SPK, -- generate unique hashed SPK
    *
FROM
    cte_bronze_raw_reviews
WHERE
    REVIEW_TEXT IS NOT NULL

-- Define how to implement incremental load: basic recent date implementation 
{#
{% if is_incremental() %}
  AND REVIEW_DATE > ( SELECT MAX(REVIEW_DATE) FROM {{this}} ) --this = actual model
{% endif %}
#}

-- Define how to implement incremental load: advanced specific implementation with dbt variables definition
{% if is_incremental() %}
    {% if var("start_date", False) and var("end_date", False) %}
        {{ log('Loading ' ~ this ~ 'incrementally (start_date: ' ~ var("start_date") ~ ', end_date: ' ~ var("end_date") ~ ')', info=True) }}
        AND REVIEW_DATE >= '{{ var("start_date") }}'
        AND REVIEW_DATE < '{{ var("end_date") }}'
    {% else %}
        AND REVIEW_DATE > ( SELECT MAX(REVIEW_DATE) FROM {{this}} )
        {{ log('Loading ' ~ this ~ ' incrementally (we take into account all recent dates) ', info=True) }} -- this does not allow you to implement backfills
    {% endif %}       
{% endif %}


-- In case you want to rebuild the whole table from scratch, and if you change the schema => dbt run --full-refresh