{{ config(materialized='view') }}

SELECT 
    agent_id,
    agent_name,
    COUNT(*) AS claim_count,
    SUM(claim_amount) AS total_claim_amount,
    SUM(claim_paid_amount) AS total_paid_amount,
    AVG(claim_amount) AS avg_claim_amount
FROM {{ ref('silver_claims') }}
GROUP BY agent_id, agent_name
ORDER BY total_paid_amount DESC