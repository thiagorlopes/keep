{{ config(materialized='view') }}

SELECT *
FROM read_parquet('../data_lake/silver/*/*.parquet')
