{{ config(materialized='view') }}

SELECT 
    claim_status AS status,
    COUNT(*) AS claim_count,
    SUM(claim_amount) AS total_claim_amount,
    SUM(claim_paid_amount) AS total_paid_amount,
    AVG(claim_amount) AS avg_claim_amount,
    AVG(claim_paid_amount) AS avg_paid_amount
FROM {{ ref('silver_claims') }}
GROUP BY claim_status