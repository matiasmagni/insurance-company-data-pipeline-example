{{ config(materialized='view') }}

SELECT 
    claim_type AS business_line,
    COUNT(*) AS total_claims,
    SUM(claim_amount) AS total_amount
FROM {{ source('clickhouse_silver', 'silver_claims') }}
GROUP BY claim_type
