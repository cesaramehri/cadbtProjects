WITH cte_RAW_REVIEWS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_REVIEWS') }}
)

SELECT  
    *,
    current_timestamp() AS load_date
FROM    
    cte_RAW_REVIEWS