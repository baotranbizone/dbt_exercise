{{
    config(
        materialized='incremental'
    )
}}

with

    orders as (
        select * from {{ ref("stg_jaffle_shop__orders") }}
    ),

    payments as (
        select * from {{ ref("stg_stripe__payments") }}
    ),

    order_payments as (

        select
            order_id,
            sum(case when payment_status = 'success' then payment_amount end) as amount
        from payments
        group by 1

    ),

    final as (

        select
            orders.order_id,
            orders.customer_id,
            orders.order_placed_at,
            orders.order_status,
            coalesce(order_payments.amount, 0) as amount
        from orders
        left join order_payments using (order_id)

    )

select * from final
{% if is_incremental() -%}
    where order_placed_at >= (select max(order_placed_at) from {{ this }}) 
{%- endif %}
order by order_placed_at desc
