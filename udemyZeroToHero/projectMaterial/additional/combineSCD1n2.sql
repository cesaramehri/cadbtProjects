{{
    config(
        materialized='incremental',
        unique_key = ['Merge_key']
    )
}}

WITH source_data AS (
    -- Get all source records
    SELECT
        Customer_ID,
        Is_Deleted,
        Customer_Name,
        Phone_Number,
        Email,
        Account_Manager_ID,
        Billing_Address,
        current_timestamp() AS load_date
    FROM {{ source('customer_accounts') }}
),

scd_update AS (
    -- Join source data with the existing data from the target table
    SELECT
        s.Customer_ID,
        s.Is_Deleted,
        s.Customer_Name,
        s.Phone_Number,
        s.Email,
        s.Account_Manager_ID,
        s.Billing_Address,
        s.load_date,
        COALESCE(t.effective_start_date, s.load_date) AS effective_start_date,
        t.effective_end_date,
        t.is_current,
        t.merge_key,

        -- Detect SCD-2 changes (Account_Manager_ID, Billing_Address)
        CASE 
            WHEN t.Customer_ID IS NULL THEN TRUE  -- New record
            WHEN s.Account_Manager_ID <> t.Account_Manager_ID OR s.Billing_Address <> t.Billing_Address THEN TRUE
            ELSE FALSE
        END AS is_scd2_change,
        
        -- Detect SCD-1 changes (Customer_Name, Phone_Number, Email)
        CASE 
            WHEN t.Customer_ID IS NOT NULL AND (
                s.Customer_Name <> t.Customer_Name OR 
                s.Phone_Number <> t.Phone_Number OR
                s.Email <> t.Email
            ) THEN TRUE
            ELSE FALSE
        END AS is_scd1_change
    FROM source_data s
    LEFT JOIN {{ this }} t
    ON s.Customer_ID = t.Customer_ID AND t.is_current and t.merge_key is not null
),

scd_final AS (
    -- Insert new records for SCD-2 changes
    SELECT 
        Customer_ID as Merge_key,
        Customer_ID,
        Is_Deleted,
        Customer_Name,
        Phone_Number,
        Email,
        Account_Manager_ID,
        Billing_Address,
        load_date AS effective_start_date,
        NULL AS effective_end_date,
        TRUE AS is_current
    FROM scd_update
    WHERE is_scd2_change
    
    UNION ALL

    -- Close the current record for SCD-2 changes
    SELECT 
        null as Merge_key,
        t.Customer_ID,
        t.Is_Deleted,
        t.Customer_Name,
        t.Phone_Number,
        t.Email,
        t.Account_Manager_ID,
        t.Billing_Address,
        t.effective_start_date,
        s.load_date AS effective_end_date,
        FALSE AS is_current
    FROM scd_update s
    JOIN {{ this }} t ON s.Customer_ID = t.Customer_ID
    WHERE s.is_scd2_change AND t.is_current = TRUE

    UNION ALL

    -- For SCD-1 changes, update the record but do not insert new rows
    SELECT 
        Customer_ID as Merge_key,
        Customer_ID,
        Is_Deleted,
        Customer_Name,
        Phone_Number,
        Email,
        Account_Manager_ID,
        Billing_Address,
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