-- Final mart model for analytic purposes
-- This model provides a summary of patient demographics and their treatment costs
WITH patient_summary AS (
    SELECT 
        patient_id,
        patient_name,
        age,
        gender,
        diagnosis,
        treatment,
        medication,
        doctor_name,
        hospital,
        admission_date,
        discharge_date,
        treatment_cost,
        recovery_status
    FROM
        {{ ref('stg_medical_records') }}
)

SELECT 
    gender,
    COUNT(*) AS patient_count,
    SUM(treatment_cost) AS total_treatment_cost,
    AVG(treatment_cost) AS average_treatment_cost
FROM 
    patient_summary
GROUP BY 
    gender;