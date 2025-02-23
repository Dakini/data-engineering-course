{{ config(materialized='table') }}

WITH trips_data AS (
    SELECT * 
    FROM {{ ref('fct_dim_taxi') }}
    WHERE fare_amount > 0 
      AND trip_distance > 0 
      AND payment_type_description IN ('Cash', 'Credit card')
)

SELECT
    service_type,
    year,
    month,
    fare_amount,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS percentile_97,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS percentile_95,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS percentile_90
FROM trips_data