{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fct_dim_taxi') }}
),

quarterly_revenue as (
    select 
        year as revenue_year,
        quarter as revenue_quarter,
        service_type, 
        sum(total_amount) as quarterly_revenue
    from trips_data
    group by year, quarter, service_type
),

yoy_growth as (
    select 
        curr.revenue_year,
        curr.revenue_quarter,
        curr.service_type,
        curr.quarterly_revenue,
        prev.quarterly_revenue as prev_year_revenue,
        -- YoY Growth Calculation
        case 
            when prev.quarterly_revenue is not null 
            then ((curr.quarterly_revenue - prev.quarterly_revenue) / prev.quarterly_revenue) * 100
            else null 
        end as yoy_growth_percentage
    from quarterly_revenue curr
    left join quarterly_revenue prev
        on curr.service_type = prev.service_type
        and curr.revenue_quarter = prev.revenue_quarter
        and curr.revenue_year = prev.revenue_year + 1
)

select * from yoy_growth
