with garmin as (
  select * from {{ ref('stg_garmin_hr_samples') }}
),
coospo as (
  select * from {{ ref('stg_coospo_hr_samples') }}
)

select * from garmin
union all
select * from coospo
