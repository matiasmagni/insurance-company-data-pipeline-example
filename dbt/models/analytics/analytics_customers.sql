{{ config(materialized='view') }}

SELECT * FROM {{ source('clickhouse_silver', 'silver_customers') }}
