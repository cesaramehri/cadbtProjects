{{
    config(
        materialized='incremental',
        unique_key = ['CUSTOMER_SPK']
    )
}}



WITH source_data AS
(
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS CUSTOMER_SPK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        current_timestamp() as effective_start_date,
        null as effective_end_date,
        TRUE as is_current,
        current_timestamp() AS load_date
    FROM
        {{ ref('stg_jaffle_shop__customers') }}
)

{% if is_incremental() %}
,scd_update AS (
    SELECT
        s.CUSTOMER_SPK as CUSTOMER_SPK,
        s.CUSTOMER_ID as CUSTOMER_ID,
        s.FIRST_NAME as FIRST_NAME,
        s.LAST_NAME as LAST_NAME,
        s.load_date as load_date,
        
        t.effective_start_date as effective_start_date,
        t.effective_end_date as effective_end_date,
        t.is_current as is_current,

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
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'first_name']) }} AS CUSTOMER_SPK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        load_date AS effective_start_date,
        NULL AS effective_end_date,
        TRUE AS is_current,
        load_date
    FROM 
        scd_update
    WHERE 
        is_scd2_change
    
    {% if is_incremental() %}
    UNION ALL

    -- Close the current record for SCD-2 changes
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['t.customer_id']) }} AS CUSTOMER_SPK,
        t.CUSTOMER_ID,
        t.FIRST_NAME,
        t.LAST_NAME,
        t.effective_start_date,
        s.load_date AS effective_end_date,
        FALSE AS is_current,
        s.load_date
    FROM scd_update s
    JOIN {{ this }} t ON s.CUSTOMER_ID = t.CUSTOMER_ID
    WHERE s.is_scd2_change AND t.is_current = TRUE
    {% endif %}

    UNION ALL

    -- For SCD-1 changes, update the record but do not insert new rows
    SELECT 
        CUSTOMER_SPK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        effective_start_date,
        effective_end_date,
        TRUE AS is_current,
        load_date
    FROM scd_update
    WHERE is_scd1_change AND is_scd2_change = FALSE
)
{% endif %}

{% if is_incremental() %}
    SELECT * FROM scd_final
{% else %}
    select * from source_data
{% endif %}

{# {% endif %} #}