with cte_dim_customers as (
    select
        *
    from
        {{ ref('dim_customers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'first_name', 'last_name']) }} AS CUSTOMER_SPK,
    *
from 
    cte_dim_customers