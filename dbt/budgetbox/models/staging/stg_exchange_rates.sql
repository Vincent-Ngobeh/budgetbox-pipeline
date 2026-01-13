{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

/*
    Staging: Exchange Rates
    
    Cleans and standardizes exchange rate data from Frankfurter API.
*/

with source as (
    select * from {{ source('raw', 'exchange_rates') }}
),

cleaned as (
    select
        -- Date
        cast(rate_date as date) as rate_date,
        
        -- Currencies (uppercase)
        upper(trim(base_currency)) as base_currency,
        upper(trim(target_currency)) as target_currency,
        
        -- Rates
        cast(rate as decimal(18, 6)) as rate,
        cast(inverse_rate as decimal(18, 6)) as inverse_rate,
        
        -- Metadata
        _ingested_at,
        _source

    from source
    where rate is not null
      and rate > 0
)

select * from cleaned
