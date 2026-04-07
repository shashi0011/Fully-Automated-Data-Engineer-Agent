-- Staging model for medical records
-- This model pulls data from the raw medical records table
WITH raw_data AS (
    SELECT 
        patient_id,
        patient_name,
        age,
        gender,
        diagnosis,
        treatment,
        medication,
        doctor_name,
        department,
        hospital,
        admission_date,
        discharge_date,
        treatment_cost,
        recovery_status
    FROM
        {{ ref('medical_records_raw') }}
)

SELECT *
FROM raw_data;