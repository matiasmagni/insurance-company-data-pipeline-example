{{ config(materialized='view') }}

SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    date_of_birth,
    address,
    city,
    state,
    zip_code,
    country,
    credit_score,
    annual_income,
    occupation,
    created_at,
    updated_at,
    
    CASE 
        WHEN credit_score >= 750 THEN 'Excellent'
        WHEN credit_score >= 700 THEN 'Good'
        WHEN credit_score >= 650 THEN 'Fair'
        ELSE 'Poor'
    END AS risk_bucket
    
FROM {{ source('minio_raw', 'customers') }}