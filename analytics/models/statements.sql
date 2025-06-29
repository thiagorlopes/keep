{{ config(materialized='view') }}

-- This model reads the Parquet files directly from the silver Delta table directory.
-- This is a reliable way to read the data, though it bypasses the Delta transaction log.

SELECT *
FROM read_parquet('../data_lake/silver/*.parquet', union_by_name=true)
