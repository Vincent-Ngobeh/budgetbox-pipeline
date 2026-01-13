{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

/*
    Fact: Transactions
    
    Central fact table with all financial transactions.
    Includes GBP-converted amounts from intermediate layer.
    
    Grain: One row per transaction
*/

with transactions as (
    select * from {{ ref('int_transactions_with_gbp') }}
),

dim_accounts as (
    select * from {{ ref('dim_accounts') }}
),

dim_categories as (
    select * from {{ ref('dim_categories') }}
),

dim_dates as (
    select * from {{ ref('dim_dates') }}
),

fact_transactions as (
    select
        -- Surrogate key
        row_number() over (order by t.transaction_timestamp) as transaction_key,
        
        -- Natural key
        t.transaction_id,
        
        -- Foreign keys
        d.date_key,
        a.account_key,
        c.category_key,
        
        -- Transaction attributes
        t.merchant,
        t.description,
        t.transaction_type,
        
        -- Amounts (original currency)
        t.amount,
        t.signed_amount,
        t.currency,
        
        -- Amounts (GBP converted)
        t.amount_gbp,
        t.signed_amount_gbp,
        
        -- Conversion metadata
        t.exchange_rate_used,
        t.exchange_rate_date,
        t.was_converted,
        
        -- Timestamps
        t.transaction_date,
        t.transaction_timestamp,
        
        -- Audit
        t._ingested_at,
        current_timestamp as _created_at
        
    from transactions t
    left join dim_accounts a
        on t.account_id = a.account_id
    left join dim_categories c
        on t.category = c.category_id
    left join dim_dates d
        on t.transaction_date = d.full_date
)

select * from fact_transactions
