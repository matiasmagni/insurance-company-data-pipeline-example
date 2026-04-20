{{ config(materialized='view') }}

SELECT 
    claim_id,
    customer_id,
    claim_date,
    claim_type,
    claim_status,
    claim_amount,
    claim_paid_amount,
    vehicle_type,
    agent_id,
    agent_name,
    created_at,
    updated_at,
    
    CASE 
        WHEN claim_status = 'Closed' THEN 'Closed'
        WHEN claim_status = 'Denied' THEN 'Denied'
        WHEN claim_status = 'Open' THEN 'Open'
        ELSE 'Other'
    END AS claim_status_category,
    
    CASE
        WHEN vehicle_type IN ('Sedan', 'Coupe', 'Wagon') THEN 'Car'
        WHEN vehicle_type = 'SUV' THEN 'SUV'
        WHEN vehicle_type = 'Truck' THEN 'Truck'
        WHEN vehicle_type = 'Motorcycle' THEN 'Motorcycle'
        ELSE 'Other'
    END AS vehicle_category
    
FROM {{ source('minio_raw', 'claims') }}