{{ config(materialized='view') }}

SELECT 
    vehicle_type,
    vehicle_category,
    COUNT(*) AS claim_count,
    SUM(claim_amount) AS total_claim_amount,
    SUM(claim_paid_amount) AS total_paid_amount,
    AVG(claim_amount) AS avg_claim_amount
FROM {{ ref('silver_claims') }}
GROUP BY vehicle_type, vehicle_category
ORDER BY total_paid_amount DESC