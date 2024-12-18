WITH cte_stg_src_raw_listings AS
(
    SELECT 
        *
    FROM 
        {{ ref('stg_src_raw__listings') }}
        --{{ ref('scd_type2_raw_listings') }}
)

SELECT
    id AS LISTING_ID,
    NAME AS LISTING_NAME,
    LISTING_URL,
    ROOM_TYPE,
    MINIMUM_NIGHTS,
    HOST_ID,
    PRICE AS PRICE_STR,
    CREATED_AT,
    UPDATED_AT
FROM
    cte_stg_src_raw_listings