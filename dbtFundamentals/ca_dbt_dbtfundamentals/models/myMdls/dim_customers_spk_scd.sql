{{
    config(
        materialized='incremental',
        unique_key = ['CUSTOMER_SPK']
    )
}}

{#
{% if is_incremental() %}
#}

WITH source_data AS
(
    SELECT
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        current_timestamp() AS load_date
    FROM
        {{ ref('stg_jaffle_shop__customers') }}
),

cte_scd_update AS (
    SELECT
        s.CUSTOMER_ID,
        s.FIRST_NAME,
        s.LAST_NAME,
        s.load_date,
        
        COALESCE(t.effective_start_date, s.load_date) AS effective_start_date,
        t.effective_end_date,
        t.is_current,
        t.CUSTOMER_SPK,

        -- Detect SCD-2 changes (FIRST_NAME)
        CASE 
            WHEN t.CUSTOMER_ID IS NULL THEN TRUE  -- New record
            WHEN s.FIRST_NAME <> t.FIRST_NAME THEN TRUE
            ELSE FALSE
        END AS is_scd2_change,
        
        -- Detect SCD-1 changes (LAST_NAME)
        CASE 
            WHEN t.CUSTOMER_ID IS NOT NULL AND (
                s.LAST_NAME <> t.LAST_NAME
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
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'first_name', 'last_name']) }} AS CUSTOMER_SPK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
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
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'first_name', 'last_name']) }} AS CUSTOMER_SPK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
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

{# {% endif %} #}