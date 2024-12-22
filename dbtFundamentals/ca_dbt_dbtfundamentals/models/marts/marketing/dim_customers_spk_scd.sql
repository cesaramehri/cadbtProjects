{{
    config(
        materialized='incremental',
        unique_key = ['CUSTOMER_SPK']
    )
}}

WITH source_data AS
(
    SELECT
        CUSTOMER_SPK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        FIRST_ORDER_DATE,
        MOST_RECENT_ORDER_DATE,
        NUMBER_OF_ORDERS,
        current_timestamp() AS load_date
    FROM
        {{ ref('dim_customers_spk') }}
),

cte_scd_update AS (
    SELECT
        s.CUSTOMER_SPK,
        s.CUSTOMER_ID,
        s.FIRST_NAME,
        s.LAST_NAME,
        s.FIRST_ORDER_DATE,
        s.MOST_RECENT_ORDER_DATE,
        s.NUMBER_OF_ORDERS,
        s.load_date,
        
        COALESCE(t.effective_start_date, s.load_date) AS effective_start_date,
        t.effective_end_date,
        t.is_current,
        t.CUSTOMER_SPK,

        -- Detect SCD-2 changes (Account_Manager_ID, Billing_Address)
        CASE 
            WHEN t.CUSTOMER_ID IS NULL THEN TRUE  -- New record
            WHEN s.FIRST_NAME <> t.FIRST_NAME OR s.LAST_NAME <> t.LAST_NAME THEN TRUE
            ELSE FALSE
        END AS is_scd2_change,
        
        -- Detect SCD-1 changes (Customer_Name, Phone_Number, Email)
        CASE 
            WHEN t.CUSTOMER_ID IS NOT NULL AND (
                s.MOST_RECENT_ORDER_DATE <> t.MOST_RECENT_ORDER_DATE OR 
                s.NUMBER_OF_ORDERS <> t.NUMBER_OF_ORDERS
            ) THEN TRUE
            ELSE FALSE
        END AS is_scd1_change
    FROM source_data s
    LEFT JOIN {{ this }} t
    ON s.CUSTOMER_ID = t.CUSTOMER_ID AND t.is_current and t.CUSTOMER_SPK is not null
),

scd_final AS (
    -- Insert new records for SCD-2 changes
    SELECT 
        CUSTOMER_SPK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        FIRST_ORDER_DATE,
        MOST_RECENT_ORDER_DATE,
        NUMBER_OF_ORDERS,
        load_date AS effective_start_date,
        NULL AS effective_end_date,
        TRUE AS is_current
    FROM 
        scd_update
    WHERE 
        is_scd2_change
    
    UNION ALL

    -- Close the current record for SCD-2 changes
    SELECT 
        null as CUSTOMER_SPK,
        t.CUSTOMER_ID,
        t.FIRST_NAME,
        t.LAST_NAME,
        t.FIRST_ORDER_DATE,
        t.MOST_RECENT_ORDER_DATE,
        t.NUMBER_OF_ORDERS,
        t.effective_start_date,
        s.load_date AS effective_end_date,
        FALSE AS is_current
    FROM scd_update s
    JOIN {{ this }} t ON s.CUSTOMER_ID = t.CUSTOMER_ID
    WHERE s.is_scd2_change AND t.is_current = TRUE

    UNION ALL

    -- For SCD-1 changes, update the record but do not insert new rows
    SELECT 
        CUSTOMER_SPK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        FIRST_ORDER_DATE,
        MOST_RECENT_ORDER_DATE,
        NUMBER_OF_ORDERS,
        effective_start_date,
        effective_end_date,
        TRUE AS is_current
    FROM scd_update
    WHERE is_scd1_change AND is_scd2_change = FALSE
)

SELECT 
    *
FROM 
    scd_final