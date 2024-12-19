WITH cte_stg_src_raw_hosts AS
(
    SELECT 
        *
    FROM 
        {{ ref('stg_src_raw__hosts') }}
)

SELECT
    ID AS HOST_ID,
    NAME AS HOST_NAME,
    IS_SUPERHOST,
    CREATED_AT,
    UPDATED_AT,
    dbt_load_date_
FROM
    cte_stg_src_raw_hosts