WITH cte_RAW_LISTINGS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_LISTINGS') }}
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
    cte_RAW_LISTINGS