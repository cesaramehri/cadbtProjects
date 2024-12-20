-- override the dbt_project.yml
{{
  config(
    materialized='table'
  )
}}



with cte_customers as (
    select
        *
    from 
        {{ ref('stg_jaffle_shop__customers') }}
),

cte_orders as (
    select
        *
    from 
        {{ ref('stg_jaffle_shop__orders') }}
),

cte_customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from cte_orders
    group by 1
),

final as (
    select
        cte_customers.customer_id,
        cte_customers.first_name,
        cte_customers.last_name,
        cte_customer_orders.first_order_date,
        cte_customer_orders.most_recent_order_date,
        coalesce(cte_customer_orders.number_of_orders, 0) as number_of_orders
    from cte_customers
    left join cte_customer_orders using (customer_id)

)

select * from final