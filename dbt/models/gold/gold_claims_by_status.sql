{{ config(materialized='view') }}

SELECT 
    claim_status_category,
    COUNT(*) AS total_claims,
    SUM(claim_amount) AS total_amount,
    AVG(claim_amount) AS avg_claim_amount
FROM {{ source('clickhouse_silver', 'silver_claims') }}
GROUP BY claim_status_category
