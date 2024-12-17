-- Singular test: In order for a singular test to succeed, it must return 0 records
-- all listings should have a minimum night of 1
-- if one single listing has a minumum night of, e.g., 0, then this test would return some records
-- and as a result, this test will fail
SELECT 
    *
FROM
    {{ ref('silver_dim_listings') }}
WHERE
    MINIMUM_NIGHTS < 1
LIMIT
    10