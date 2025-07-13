with 

source as (

    select * from {{ source('dbt_btranquoc', 'jaffle_shop_orders') }}

),

transformed as (

    select 
    
        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        status as order_status

    from source

)
select * from transformed