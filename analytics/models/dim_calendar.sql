-- This model uses the godatadriven/dbt_date package to generate a comprehensive calendar dimension table.
-- It's configured to generate dates from the beginning of 2023 to the end of 2025.

{{ config(materialized='table') }}

{{ dbt_date.get_date_dimension(
    start_date="2023-01-01",
    end_date="2025-12-31"
) }}
