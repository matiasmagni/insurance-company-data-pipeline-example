{{ config(materialized='view') }}

SELECT * FROM {{ ref('silver_customers') }}