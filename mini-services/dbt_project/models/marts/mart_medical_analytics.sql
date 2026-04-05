-- DataForge AI Generated dbt Model (fallback — no LLM)
-- Mart: Final analytics table for medical data

with staging as (
    select * from {{ ref('stg_student_performance_data_clean') }}
),

aggregated as (
    select * from {{ ref('int_medical_aggregated') }}
),

final as (
    select
        s.*,
        a.record_count,
        a.total_study_hours_per_day,
        a.avg_study_hours_per_day
    from staging s
    left join aggregated a on s.gender = a.gender
)

select * from final
