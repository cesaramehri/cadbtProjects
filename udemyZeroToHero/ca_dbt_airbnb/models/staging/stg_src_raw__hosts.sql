WITH cte_RAW_HOSTS AS (
    SELECT  
        *
    FROM    
        DBT_AIRBNB_RAW.PUBLIC.RAW_HOSTS
)

SELECT  
    ID AS HOST_ID,
    NAME AS HOST_NAME,
    IS_SUPERHOST,
    CREATED_AT,
    UPDATED_AT
FROM    
    cte_RAW_HOSTS