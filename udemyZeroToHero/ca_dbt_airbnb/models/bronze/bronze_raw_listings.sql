with cte_stg_src_raw_listings AS
(
    SELECT 
        *
    FROM 
        {{ ref('stg_src_raw__listings') }}
)

SELECT
    *
FROM
    cte_stg_src_raw_listings