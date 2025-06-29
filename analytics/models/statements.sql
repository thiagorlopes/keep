{{ config(materialized='view') }}

-- This model reads all Parquet files from the Silver layer of the data lake,
-- which contains the cleaned and standardized transaction data.

SELECT * FROM read_parquet("{{ env_var('DATA_LAKE_ROOT', '..') }}/data_lake/silver/*.parquet")
