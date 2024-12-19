WITH cte_RAW_LISTINGS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_LISTINGS') }}
)

SELECT  
   *,
   current_timestamp() AS load_date
FROM    
    cte_RAW_LISTINGS