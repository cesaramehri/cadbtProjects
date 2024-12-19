WITH cte_RAW_REVIEWS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_REVIEWS') }}
)

SELECT  
    *,
    current_timestamp() AS dbt_load_date_
FROM    
    cte_RAW_REVIEWS