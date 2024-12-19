WITH cte_RAW_LISTINGS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_LISTINGS') }}
)

SELECT  
   *,
   current_timestamp() AS dbt_load_date_
FROM    
    cte_RAW_LISTINGS