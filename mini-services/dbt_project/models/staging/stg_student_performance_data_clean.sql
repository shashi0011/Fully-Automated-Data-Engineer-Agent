-- DataForge AI Generated dbt Model (fallback — no LLM)
-- Staging: student_performance_data_clean
-- Dataset Type: medical
-- Generated at: 2026-04-05T18:50:52.689879

with source as (
    select * from {{ source('raw', 'student_performance_data_raw') }}
),

cleaned as (
    select
        -- Primary identifiers
        student_id as student_id,

        -- Dimensional columns
        trim(gender) as gender,
        trim(internet_access) as internet_access,
        trim(extra_classes) as extra_classes,

        -- Metrics
        round(study_hours_per_day::numeric, 2) as study_hours_per_day,
        round(attendance_percentage::numeric, 2) as attendance_percentage,
        round(assignment_score::numeric, 2) as assignment_score,

        -- Time dimensions
        -- No date columns

        -- Metadata
        current_timestamp as _loaded_at

    from source
    where 1=1
        and student_id is not null
)

select * from cleaned
