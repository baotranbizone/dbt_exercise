with payments as (
    
    select * from {{ ref('stg_stripe__payments') }}

),

pivoted as (

    select 

        order_id,

        {%- set methods = ['credit_card', 'coupon', 'gift_card', 'bank_transfer'] -%}

        {% for method in methods %}

        sum(case when payment_method = '{{method}}' then payment_amount else 0 end) as {{method}}_amount

            {%- if not loop.last -%}
                ,
            {%- endif %}
        
        {%- endfor %}

    from payments
    where payment_status != 'fail'
    group by 1

)

select * from pivoted