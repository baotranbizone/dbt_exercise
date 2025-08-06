with

    source as (select * from {{ source("dbt_btranquoc", "stripe_payments") }}),

    transformed as (

        select

            id as payment_id,
            orderid as order_id,
            paymentmethod as payment_method,
            created as payment_created_at,
            status as payment_status,
            {{ cents_to_dollars('amount', 2) }} as payment_amount

        from source

    )

select * from transformed