{{ config(materialized='table') }}

-- This model reads data from the Silver layer Delta table.
-- By using delta_scan(), we instruct DuckDB's delta extension
-- to correctly read the latest version of the table from the transaction log,
-- ignoring any orphaned files from previous overwrites.

SELECT * FROM delta_scan("{{ env_var('DATA_LAKE_ROOT', '..') }}/data_lake/silver")
