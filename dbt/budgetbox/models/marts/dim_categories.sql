{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

/*
    Dimension: Categories

    Transaction categories derived from transaction data.
    Includes hierarchy grouping for UK personal finance categories.
*/

with categories_from_transactions as (
    select distinct
        category,
        category_type
    from {{ ref('stg_transactions') }}
    where category is not null
),

-- Define category hierarchy for UK personal finance
category_enriched as (
    select
        category,
        category_type,
        case category
            -- Essential expenses
            when 'Rent/Mortgage' then 'Housing'
            when 'Council Tax' then 'Housing'
            when 'Utilities' then 'Housing'
            when 'Insurance' then 'Housing'
            when 'Groceries' then 'Essential'

            -- Transport
            when 'TfL/Oyster' then 'Transport'
            when 'Transport' then 'Transport'
            when 'Petrol' then 'Transport'

            -- Services
            when 'Mobile Phone' then 'Bills & Services'
            when 'Internet' then 'Bills & Services'
            when 'Subscriptions' then 'Bills & Services'

            -- Lifestyle
            when 'Eating Out' then 'Lifestyle'
            when 'Entertainment' then 'Lifestyle'
            when 'Shopping' then 'Lifestyle'
            when 'Health & Fitness' then 'Lifestyle'
            when 'Personal Care' then 'Lifestyle'
            when 'Travel' then 'Lifestyle'

            -- Income
            when 'Salary' then 'Employment Income'
            when 'Freelance' then 'Self-Employment'
            when 'Bonus' then 'Employment Income'
            when 'Other Income' then 'Other Income'

            else 'Other'
        end as parent_category,

        case category
            -- Essential vs discretionary for budgeting
            when 'Rent/Mortgage' then 'Essential'
            when 'Council Tax' then 'Essential'
            when 'Utilities' then 'Essential'
            when 'Insurance' then 'Essential'
            when 'Groceries' then 'Essential'
            when 'TfL/Oyster' then 'Essential'
            when 'Transport' then 'Semi-Essential'
            when 'Mobile Phone' then 'Essential'
            when 'Internet' then 'Essential'
            else 'Discretionary'
        end as budget_category

    from categories_from_transactions
),

categories as (
    select
        -- Surrogate key
        row_number() over (order by category_type desc, category) as category_key,

        -- Natural key
        category as category_id,

        -- Attributes
        category as category_name,
        category_type,
        parent_category,
        budget_category,

        -- For reporting sort order
        case parent_category
            when 'Employment Income' then 1
            when 'Self-Employment' then 2
            when 'Other Income' then 3
            when 'Housing' then 4
            when 'Essential' then 5
            when 'Transport' then 6
            when 'Bills & Services' then 7
            when 'Lifestyle' then 8
            else 9
        end as sort_order

    from category_enriched
)

select * from categories
