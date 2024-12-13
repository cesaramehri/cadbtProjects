WITH cte_bronze_raw_hosts AS
(
    SELECT
        *
    FROM 
        {{ ref('bronze_raw_hosts') }}
)

SELECT
    HOST_ID,
	NVL(HOST_NAME, 'Anonymous') AS HOST_NAME,
	IS_SUPERHOST,
	CREATED_AT,
	UPDATED_AT
FROM
    cte_bronze_raw_hosts