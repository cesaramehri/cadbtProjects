WITH cte_silver_fact_reviews AS
(
    SELECT
        *
    FROM
        {{ ref('silver_fact_reviews') }}
),
cte_full_moon_dates AS
(
    SELECT
        *
    FROM
        {{ ref('seed_full_moon_dates') }}
)

SELECT 
    r.*,
    CASE 
        WHEN fm.full_moon_date IS NULL 
            THEN 'not full moon'
        ELSE 'full moon'
    END AS is_full_moon
FROM 
    cte_silver_fact_reviews r
LEFT JOIN
    cte_full_moon_dates fm
ON
    (TO_DATE(r.REVIEW_DATE) = DATEADD(DAY, 1, fm.full_moon_date))