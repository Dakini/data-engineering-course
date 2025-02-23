{{ config(materialized='table') }}

with trips_data as (
    select *,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from {{ ref('dim_fhv_trips') }}
)

select
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,

    month,
    year,
    trip_duration,
    pickup_datetime,
    dropoff_datetime,
    PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) AS percentile_90
from trips_data
