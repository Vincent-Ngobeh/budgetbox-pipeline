{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

/*
    Staging model for transactions.

    This model:
    - Selects from raw source
    - Cleans and standardizes data
    - Casts data types explicitly
    - Adds no business logic (that's for marts)
*/

with source as (
    select * from {{ source('raw', 'transactions') }}
),

staged as (
    select
        -- Primary key
        transaction_id,

        -- Account info
        account_id,
        trim(account_name) as account_name,
        lower(account_type) as account_type,
        trim(bank_name) as bank_name,
        upper(currency) as currency,

        -- Transaction amounts
        cast(amount as decimal(18, 2)) as amount,
        cast(signed_amount as decimal(18, 2)) as signed_amount,

        -- Transaction attributes
        lower(transaction_type) as transaction_type,
        trim(category) as category,
        lower(category_type) as category_type,
        trim(merchant) as merchant,
        trim(description) as description,

        -- Dates
        cast(transaction_date as date) as transaction_date,
        cast(transaction_timestamp as timestamp) as transaction_timestamp,
        transaction_year,
        transaction_month,
        transaction_day_of_week,

        -- Metadata
        _ingested_at,
        _source

    from source

    -- Basic data quality filter
    where transaction_id is not null
      and amount > 0
)

select * from staged
