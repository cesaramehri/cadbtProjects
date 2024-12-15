

with cte_stg_src_raw_hosts AS
(
    SELECT 
        *
    FROM 
        {{ ref('stg_src_raw__hosts') }}
)

SELECT
    *
FROM
    cte_stg_src_raw_hosts