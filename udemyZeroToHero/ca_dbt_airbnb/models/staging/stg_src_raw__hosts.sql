WITH cte_RAW_HOSTS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_HOSTS') }}
)

SELECT  
    *,
    current_timestamp() AS load_date
FROM    
    cte_RAW_HOSTS