with cte_stg_src_raw_reviews AS
(
    SELECT 
        *
    FROM 
        {{ ref('stg_src_raw__reviews') }}
)

SELECT
    *
FROM
    cte_stg_src_raw_reviews