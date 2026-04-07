-- Intermediate model for processing medical records
-- This model aggregates treatment costs by department to provide insights on financial metrics
WITH department_costs AS (
    SELECT 
        department,
        SUM(treatment_cost) AS total_treatment_cost,
        COUNT(DISTINCT patient_id) AS patient_count
    FROM
        {{ ref('stg_medical_records') }}
    GROUP BY
        department
)

SELECT 
    department,
    total_treatment_cost,
    patient_count,
    total_treatment_cost / NULLIF(patient_count, 0) AS average_treatment_cost
FROM 
    department_costs;