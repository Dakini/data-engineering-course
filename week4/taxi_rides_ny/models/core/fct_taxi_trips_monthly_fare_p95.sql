{{ config(materialized='table') }}

WITH trips_data AS (
    SELECT 
    service_type, 
    year, 
    month,
    SUM(fare_amount) as total_amount

    FROM {{ ref('fct_dim_taxi') }}
    WHERE fare_amount > 0 
      AND trip_distance > 0 
      AND payment_type_description IN ('Cash', 'Credit Card')
)
from trips_data
GROUP BY service_type, year, month 
-- SELECT
--     service_type,
--     year,
--     month,
--     PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS percentile_97,
--     PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS percentile_95,
--     PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS percentile_90
-- FROM trips_data
