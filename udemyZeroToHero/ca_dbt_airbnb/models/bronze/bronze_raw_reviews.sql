WITH cte_stg_src_raw_reviews AS
(
    SELECT 
        *
    FROM 
        {{ ref('stg_src_raw__reviews') }}
)

SELECT
    LISTING_ID,
    "DATE" AS REVIEW_DATE,
    REVIEWER_NAME,
    COMMENTS AS REVIEW_TEXT,
    SENTIMENT AS REVIEW_SENTIMENT,
    dbt_load_date_
FROM
    cte_stg_src_raw_reviews