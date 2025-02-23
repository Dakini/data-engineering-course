{{ config(materialized='table') }}

with trips_data as (
    select
        *,
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
    PERCENTILE_CONT(trip_duration, 0.90) WITHIN GROUP (ORDER BY trip_duration)
        OVER (PARTITION BY year, month, pickup_location_id, dropoff_location_id) as percentile_90
from trips_data
