-- A listing must already exist before a review exists (because is a review is made on a listing)
SELECT 
    *
FROM
    {{ ref('silver_fact_reviews') }} AS FR
JOIN
    {{ ref('silver_dim_listings') }} AS DL
USING 
    (LISTING_ID)
WHERE
    FR.REVIEW_DATE <= DL.CREATED_AT