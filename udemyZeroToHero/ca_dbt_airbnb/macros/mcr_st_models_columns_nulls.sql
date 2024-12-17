{% macro mcr_st_models_columns_nulls(MODEL_NAME) %}

    SELECT * FROM {{ MODEL_NAME }} WHERE
    {% for col in adapter.get_columns_in_relation(MODEL_NAME) -%}   -- '-' to trim white spaces at the end of the template
        {{ col.column }} IS NULL OR     --next column is null OR next column etc.
    {% endfor %}
    
    FALSE       -- end OR

{%- endmacro %}