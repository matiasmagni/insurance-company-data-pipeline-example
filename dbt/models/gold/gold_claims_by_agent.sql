{{ config(materialized='view') }}

SELECT 
    agent_id,
    COUNT(*) AS total_claims,
    SUM(claim_amount) AS total_amount,
    AVG(claim_amount) AS avg_claim_amount
FROM {{ source('clickhouse_silver', 'silver_claims') }}
WHERE agent_id IS NOT NULL
GROUP BY agent_id
