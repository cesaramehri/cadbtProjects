{% test mcr_st_silver_dim_listings_min_nights(model, column_name) %}

SELECT 
    *
FROM
    {{ model }}
WHERE
    {{ column_name }} < 1

{% endtest%}