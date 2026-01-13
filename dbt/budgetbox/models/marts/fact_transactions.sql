{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

/*
    Fact: Transactions

    Central fact table containing all financial transactions
    with foreign keys to dimension tables.

    Grain: One row per transaction

    Note: This model is independent and does not require exchange rates.
    Currency conversion can be added in a later iteration.
*/

with transactions as (
    select * from {{ ref('stg_transactions') }}
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

        -- Foreign keys to dimensions
        d.date_key,
        a.account_key,
        c.category_key,

        -- Degenerate dimensions (transaction-level attributes)
        t.merchant,
        t.description,
        t.transaction_type,

        -- Measures (facts)
        t.amount,
        t.signed_amount,
        t.currency,

        -- For future GBP conversion (currently same as amount since all GBP)
        t.amount as amount_gbp,
        t.signed_amount as signed_amount_gbp,

        -- Date/time
        t.transaction_date,
        t.transaction_timestamp,

        -- Audit columns
        t._ingested_at,
        current_timestamp as _created_at

    from transactions t

    -- Join to dimensions
    left join dim_accounts a
        on t.account_id = a.account_id

    left join dim_categories c
        on t.category = c.category_id

    left join dim_dates d
        on t.transaction_date = d.full_date
)

select * from fact_transactions
