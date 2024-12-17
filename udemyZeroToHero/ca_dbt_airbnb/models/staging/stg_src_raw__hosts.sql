WITH cte_RAW_HOSTS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_HOSTS') }}
)

SELECT  
    *
FROM    
    cte_RAW_HOSTS