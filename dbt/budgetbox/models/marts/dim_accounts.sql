{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

/*
    Dimension: Accounts

    Contains unique accounts derived from transaction data.
    In a production system, this would come from a dedicated accounts source.
*/

with accounts_from_transactions as (
    select distinct
        account_id,
        account_name,
        account_type,
        bank_name,
        currency
    from {{ ref('stg_transactions') }}
),

accounts as (
    select
        -- Surrogate key
        row_number() over (order by account_id) as account_key,

        -- Natural key
        account_id,

        -- Attributes
        account_name,
        account_type,
        bank_name,
        currency,

        -- Display formatting
        case account_type
            when 'current' then 'Current Account'
            when 'savings' then 'Savings Account'
            when 'credit' then 'Credit Card'
            else account_type
        end as account_type_display,

        -- Metadata
        current_timestamp as created_at,
        true as is_active

    from accounts_from_transactions
)

select * from accounts
