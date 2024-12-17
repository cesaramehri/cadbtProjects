{% test mcr_cgt_models_column_positive(model, column_name) %}

SELECT 
    *
FROM
    {{ model }}
WHERE
    {{ column_name }} < 1

{% endtest%}