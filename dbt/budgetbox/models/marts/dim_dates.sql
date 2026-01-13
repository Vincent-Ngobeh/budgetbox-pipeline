{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

/*
    Dimension: Dates

    A date dimension table for time-based analytics.
    Generates dates from 2020 to 2027 to cover historical and future analysis.

    Uses DuckDB's generate_series instead of dbt_utils.date_spine.
*/

with date_spine as (
    select
        cast(unnest(generate_series(
            date '2020-01-01',
            date '2027-12-31',
            interval '1 day'
        )) as date) as date_day
),

dates as (
    select
        -- Surrogate key (YYYYMMDD format)
        cast(strftime(date_day, '%Y%m%d') as integer) as date_key,

        -- Full date
        date_day as full_date,

        -- Day attributes
        extract(dayofweek from date_day) as day_of_week,
        case extract(dayofweek from date_day)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,
        extract(day from date_day) as day_of_month,
        extract(dayofyear from date_day) as day_of_year,

        -- Week attributes
        extract(week from date_day) as week_of_year,

        -- Month attributes
        extract(month from date_day) as month_number,
        case extract(month from date_day)
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name,
        left(case extract(month from date_day)
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end, 3) as month_short,

        -- Quarter attributes
        extract(quarter from date_day) as quarter_number,
        'Q' || cast(extract(quarter from date_day) as varchar) as quarter_name,

        -- Year attributes
        extract(year from date_day) as year_number,

        -- Fiscal year (UK: April start)
        case
            when extract(month from date_day) >= 4 then extract(year from date_day)
            else extract(year from date_day) - 1
        end as fiscal_year,

        -- Flags
        case
            when extract(dayofweek from date_day) in (0, 6) then true
            else false
        end as is_weekend,

        -- ISO format string
        strftime(date_day, '%Y-%m-%d') as iso_date,

        -- Year-Month for grouping
        strftime(date_day, '%Y-%m') as year_month

    from date_spine
)

select * from dates
order by date_key
