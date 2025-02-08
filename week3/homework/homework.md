1, 20,332,093
2, 0 MB for the External Table and 155.12 MB for the Materialized Table
3, BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
4, 8333
5, Partition by tpep_dropoff_datetime and Cluster on VendorID
6, 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
7, GCP Bucket
8, False

9, 2.72GB. because the wildcard \* will grab all the data from the tables it is gathering all the data from the table!
