-- override the dbt_project.yml
{{
  config(
    materialized='table'
  )
}}



with cte_customers as (
    select
        id as customer_id,
        first_name,
        last_name
    from DBT_DBTFUNDAMENTALS_RAW.jaffle_shop.customers
),

cte_orders as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status
    from DBT_DBTFUNDAMENTALS_RAW.jaffle_shop.orders
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