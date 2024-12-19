{% macro mcr_variables() %}

    {# Jinja variables #}
    {% set jinja_var_name = 'Cesar_jinja'%}
    {{ log("Hello, my name is: " ~ jinja_var_name, info=True) }}

    {#dbt Variables#} 
    {# dbt run-operation mcr_variables --vars "{dbt_var_user_name: Cesar_dbt}" #}
    {{ log("Hello, my name is: " ~ var("dbt_var_user_name", "default_user_name_macro") ~ "!", info=True) }}

{% endmacro%}