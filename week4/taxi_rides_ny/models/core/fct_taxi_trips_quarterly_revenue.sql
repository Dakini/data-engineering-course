{{
    config(
        materialized='table'
    )
}}

with base_data as (
    select 
        service_type,
        extract(year from pickup_datetime) as revenue_year,
        extract(quarter from pickup_datetime) as revenue_quarter,
        sum(total_amount) as total_quarterly_revenue
    from {{ ref('fct_dim_taxi') }}  -- Use ref for better dbt structure
    group by 1, 2, 3
),

yoy_growth as (
    select 
        b1.service_type,
        b1.revenue_year,
        b1.revenue_quarter,
        b1.total_quarterly_revenue,

        -- Get previous year's revenue for the same quarter
        b2.total_quarterly_revenue as prev_year_revenue,

        -- Compute YoY Growth: (Current - Previous) / Previous * 100 using SAFE_DIVIDE
        SAFE_DIVIDE(
            b1.total_quarterly_revenue - b2.total_quarterly_revenue, 
            b2.total_quarterly_revenue
        ) * 100 as yoy_revenue_growth

    from base_data b1
    left join base_data b2
        on b1.service_type = b2.service_type
        and b1.revenue_quarter = b2.revenue_quarter
        and b1.revenue_year = b2.revenue_year + 1  -- Compare to previous year's same quarter
)

select 
    service_type,
    revenue_year,
    revenue_quarter,
    cast(revenue_year as string) || '/Q' || cast(revenue_quarter as string) as year_quarter,
    total_quarterly_revenue,
    yoy_revenue_growth
from yoy_growth
order by service_type, revenue_year, revenue_quarter;
