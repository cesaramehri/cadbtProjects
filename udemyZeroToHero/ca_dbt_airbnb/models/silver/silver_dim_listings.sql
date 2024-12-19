with cte_bronze_raw_listings AS
(
    SELECT
        *
    FROM 
        {{ ref('bronze_raw_listings') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['LISTING_ID', 'LISTING_NAME']) }} AS LISTING_ID_SPK,        -- generate unique hashed SPK
    LISTING_ID,
    LISTING_NAME,
    ROOM_TYPE,
    CASE
        WHEN MINIMUM_NIGHTS = 0 THEN 1
        ELSE MINIMUM_NIGHTS
    END AS MINIMUM_NIGHTS,
    HOST_ID,
    REPLACE(PRICE_STR, '$')::NUMBER(10,2) AS PRICE,
    CREATED_AT,
    UPDATED_AT
FROM
    cte_bronze_raw_listings