WITH cte_silver_dim_listings AS
(
    SELECT
        *
    FROM
        {{ ref('silver_dim_listings') }}
),
cte_silver_dim_hosts AS
(
    SELECT
        *
    FROM
        {{ ref('silver_dim_hosts') }}
)

SELECT
    L.LISTING_ID,
    L.LISTING_NAME,
    L.ROOM_TYPE,
	L.MINIMUM_NIGHTS,
    L.PRICE,
	L.HOST_ID,
	H.HOST_NAME,
	H.IS_SUPERHOST AS HOST_IS_SUPERHOST,
	L.CREATED_AT,
    GREATEST(L.UPDATED_AT, H.UPDATED_AT) AS UPDATED_AT,
    L.dbt_load_date_
FROM
    cte_silver_dim_listings L
LEFT JOIN
    cte_silver_dim_hosts H
ON
    L.HOST_ID = H.HOST_ID