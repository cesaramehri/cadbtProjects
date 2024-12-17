WITH cte_RAW_REVIEWS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_REVIEWS') }}
)

SELECT  
    *
FROM    
    cte_RAW_REVIEWS