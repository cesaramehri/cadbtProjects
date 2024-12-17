WITH cte_RAW_LISTINGS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_LISTINGS') }}
)

SELECT  
   *
FROM    
    cte_RAW_LISTINGS