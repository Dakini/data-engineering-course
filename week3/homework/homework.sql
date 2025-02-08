-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `your project id.nytaxi.external_homework_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your bucket /homework3/yellow_tripdata_2024-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE your project id.nytaxi.homework_yellow_tripdata_non_partitoned AS
SELECT * FROM your project id.nytaxi.external_homework_yellow_tripdata;

-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT COUNT(DISTINCT PULocationID) FROM your project id.nytaxi.homework_yellow_tripdata_non_partitoned
UNION ALL
SELECT COUNT(DISTINCT PULocationID) FROM your project id.nytaxi.external_homework_yellow_tripdata

-- Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

SELECT PULocationID 
FROM your project id.nytaxi.homework_yellow_tripdata_non_partitoned 

SELECT PULocationID, DOLocationID
FROM your project id.nytaxi.homework_yellow_tripdata_non_partitoned 

-- How many records have a fare_amount of 0?
SELECT count(*) as trips
FROM your project id.nytaxi.homework_yellow_tripdata_non_partitoned
WHERE  fare_amount=0.0;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE your project id.nytaxi.homework_yellow_tripdata_partitoned
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID
AS
SELECT * 
FROM your project id.nytaxi.homework_yellow_tripdata_non_partitoned;

-- What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
SELECT count(*) FROM  your project id.nytaxi.homework_yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-03-31'


SELECT count(*) FROM your project id.nytaxi.homework_yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-03-31'

-- Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
SELECT count(DISTINCT VendorID) FROM your project id.nytaxi.homework_yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15'

SELECT count(DISTINCT VendorID) FROM your project id.nytaxi.homework_yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15'


-- No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
SELECT * FROM your project id.nytaxi.homework_yellow_tripdata_non_partitoned

