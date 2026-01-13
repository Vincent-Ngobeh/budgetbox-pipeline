{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

/*
    Intermediate: Transactions with GBP conversion.
    
    Joins transactions with exchange rates to provide
    standardized GBP amounts for all transactions.
    
    Logic:
    - GBP transactions: use original amount
    - USD/EUR transactions: convert using closest prior rate
*/

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

exchange_rates as (
    select * from {{ ref('stg_exchange_rates') }}
),

-- Get the closest exchange rate on or before transaction date
rates_ranked as (
    select
        t.transaction_id,
        t.currency,
        t.transaction_date,
        e.rate,
        e.rate_date,
        row_number() over (
            partition by t.transaction_id 
            order by e.rate_date desc
        ) as rn
    from transactions t
    left join exchange_rates e
        on t.currency = e.target_currency
        and e.base_currency = 'GBP'
        and e.rate_date <= t.transaction_date
    where t.currency != 'GBP'
),

latest_rates as (
    select
        transaction_id,
        rate,
        rate_date as exchange_rate_date
    from rates_ranked
    where rn = 1
),

transactions_with_gbp as (
    select
        t.*,
        
        -- GBP conversion
        case
            when t.currency = 'GBP' then t.amount
            when lr.rate is not null then round(t.amount / lr.rate, 2)
            else t.amount  -- Fallback: no rate available
        end as amount_gbp,
        
        case
            when t.currency = 'GBP' then t.signed_amount
            when lr.rate is not null then round(t.signed_amount / lr.rate, 2)
            else t.signed_amount
        end as signed_amount_gbp,
        
        -- Conversion metadata
        lr.rate as exchange_rate_used,
        lr.exchange_rate_date,
        case
            when t.currency = 'GBP' then false
            when lr.rate is not null then true
            else false
        end as was_converted
        
    from transactions t
    left join latest_rates lr
        on t.transaction_id = lr.transaction_id
)

select * from transactions_with_gbp
