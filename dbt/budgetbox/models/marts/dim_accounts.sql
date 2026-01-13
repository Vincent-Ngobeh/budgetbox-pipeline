{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

/*
    Dimension: Accounts
    
    Contains unique accounts derived from transaction data.
    Deduplicates by account_id, keeping the first occurrence.
*/

with accounts_from_transactions as (
    select
        account_id,
        account_name,
        account_type,
        currency,
        row_number() over (partition by account_id order by account_name) as rn
    from {{ ref('stg_transactions') }}
),

deduplicated as (
    select
        account_id,
        account_name,
        account_type,
        currency
    from accounts_from_transactions
    where rn = 1
),

accounts as (
    select
        row_number() over (order by account_id) as account_key,
        account_id,
        account_name,
        account_type,
        currency,
        current_timestamp as created_at,
        current_timestamp as updated_at,
        true as is_active
    from deduplicated
)

select * from accounts
