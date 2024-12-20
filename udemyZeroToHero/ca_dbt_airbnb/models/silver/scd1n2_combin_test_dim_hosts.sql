{{
    config(
        materialized='incremental',
        unique_key = ['Merge_key']
    )
}}


WITH cte_bronze_raw_hosts AS
(
    SELECT
        *
    FROM 
        {{ ref('bronze_raw_hosts') }}
),

cte_scd_update AS
(
    SELECT
        r.HOST_ID,
        NVL(r.HOST_NAME, 'Anonymous') AS HOST_NAME,
        r.IS_SUPERHOST,
        r.CREATED_AT,
        r.UPDATED_AT,
        r.dbt_load_date_,

        COALESCE(t.effective_start_date, r.load_date) AS effective_start_date,
        t.effective_end_date,
        t.is_current,
        t.Merge_key,

        -- Identify SCD-2 changes
        CASE 
            WHEN 
                t.HOST_ID IS NULL 
                THEN TRUE  -- New record
            WHEN 
                r.HOST_ID <> t.HOST_ID OR 
                r.HOST_NAME <> t.HOST_NAME 
                THEN TRUE
            ELSE FALSE
        END AS is_scd2_change,

        -- Identify SCD-1 changes
        CASE 
            WHEN t.HOST_ID IS NOT NULL AND (
                r.UPDATED_AT <> t.UPDATED_AT 
            ) THEN TRUE
            ELSE FALSE
        END AS is_scd1_change

    FROM
        cte_bronze_raw_hosts r
    LEFT JOIN
        {{ this }} t
    ON
        r.HOST_ID = t.HOST_ID
    AND
        (t.is_current and t.Merge_key is not null)
),

cte_scd_final AS
(
    -- Insert new records for SCD-2 changes
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['HOST_ID', 'HOST_NAME']) }} as Merge_key,
        HOST_ID,
        HOST_NAME,
        IS_SUPERHOST,
        CREATED_AT,
        UPDATED_AT,
        dbt_load_date_ AS effective_start_date,
        "2099-01-01 00:00:00" AS effective_end_date,
        TRUE AS is_current
    FROM 
        cte_scd_update
    WHERE 
        is_scd2_change
    
    UNION ALL

    -- Close the current record for SCD-2 changes
    SELECT 
        null as Merge_key,
        t.HOST_ID,
        t.HOST_NAME,
        t.IS_SUPERHOST,
        t.CREATED_AT,
        t.UPDATED_AT,
        t.effective_start_date,
        t.dbt_load_date_ AS effective_end_date,
        FALSE AS is_current
    FROM 
        cte_scd_update s
    JOIN 
        {{ this }} t 
    ON 
        s.HOST_ID = t.HOST_ID
    WHERE 
        s.is_scd2_change AND t.is_current = TRUE

    UNION ALL

    -- For SCD-1 changes, update the record but do not insert new rows
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['HOST_ID', 'HOST_NAME']) }} as Merge_key,
        HOST_ID,
        HOST_NAME,
        IS_SUPERHOST,
        CREATED_AT,
        UPDATED_AT,
        effective_start_date,
        effective_end_date,
        TRUE AS is_current
    FROM 
        cte_scd_update
    WHERE 
        is_scd1_change AND is_scd2_change = FALSE

)

SELECT *
FROM cte_scd_final




--{{ dbt_utils.generate_surrogate_key(['HOST_ID', 'HOST_NAME']) }} AS HOST_ID_SPK,        -- generate unique hashed SPK
