WITH cte_RAW_HOSTS AS (
    SELECT  
        *
    FROM    
        {{ source('airbnb', 'RAW_HOSTS') }}
)

SELECT  
    *,
    current_timestamp() AS dbt_load_date_
FROM    
    cte_RAW_HOSTS