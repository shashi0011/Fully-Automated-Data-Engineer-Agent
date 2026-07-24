-- DataForge AI Generated dbt Model (fallback — no LLM)
-- Intermediate: Aggregated analysis for medical data

with base as (
    select * from {{ ref('stg_student_performance_data_clean') }}
),

aggregated as (
    select
        gender,
        count(*) as record_count,
        sum(study_hours_per_day) as total_study_hours_per_day,
        avg(study_hours_per_day) as avg_study_hours_per_day,
        min(study_hours_per_day) as min_study_hours_per_day,
        max(study_hours_per_day) as max_study_hours_per_day
    from base
    group by gender
)

select * from aggregated
order by total_study_hours_per_day desc
