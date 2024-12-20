with cte_orders as  (
    select * from {{ ref ('stg_jaffle_shop__orders' )}}
),

cte_payments as (
    select * from {{ ref ('stg_stripe__payments') }}
),

cte_order_payments as (
    select
        order_id,
        sum (case when status = 'success' then amount end) as amount
    from 
        cte_payments
    group by 
        1
),

 final as (
    select
        cte_orders.order_id,
        cte_orders.customer_id,
        cte_orders.order_date,
        coalesce (cte_order_payments.amount, 0) as amount

    from cte_orders
    left join cte_order_payments using (order_id)
)

select * from final
