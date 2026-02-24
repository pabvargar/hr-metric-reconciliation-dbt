with hr as (
  select * from {{ ref('fct_session_hr_summary') }}
),

zones as (
  select * from {{ ref('fct_session_time_in_zone') }}
),

garmin_hr as (
  select * from hr where device = 'garmin'
),
coospo_hr as (
  select * from hr where device = 'coospo'
),

garmin_z as (
  select session_id,
         max(case when zone = 1 then pct_in_zone end) as garmin_z1,
         max(case when zone = 2 then pct_in_zone end) as garmin_z2,
         max(case when zone = 3 then pct_in_zone end) as garmin_z3,
         max(case when zone = 4 then pct_in_zone end) as garmin_z4,
         max(case when zone = 5 then pct_in_zone end) as garmin_z5
  from zones
  where device = 'garmin'
  group by 1
),

coospo_z as (
  select session_id,
         max(case when zone = 1 then pct_in_zone end) as coospo_z1,
         max(case when zone = 2 then pct_in_zone end) as coospo_z2,
         max(case when zone = 3 then pct_in_zone end) as coospo_z3,
         max(case when zone = 4 then pct_in_zone end) as coospo_z4,
         max(case when zone = 5 then pct_in_zone end) as coospo_z5
  from zones
  where device = 'coospo'
  group by 1
)

select
  s.session_id,
  s.sport,
  s.start_ts,
  s.end_ts,
  s.lthr_bpm,

  gh.avg_hr_bpm  as garmin_avg_hr,
  ch.avg_hr_bpm  as coospo_avg_hr,
  gh.max_hr_bpm  as garmin_max_hr,
  ch.max_hr_bpm  as coospo_max_hr,
  gh.p95_hr_bpm  as garmin_p95_hr,
  ch.p95_hr_bpm  as coospo_p95_hr,

  (ch.avg_hr_bpm - gh.avg_hr_bpm) as diff_avg_hr,
  (ch.max_hr_bpm - gh.max_hr_bpm) as diff_max_hr,
  (ch.p95_hr_bpm - gh.p95_hr_bpm) as diff_p95_hr,

  gz.garmin_z1, gz.garmin_z2, gz.garmin_z3, gz.garmin_z4, gz.garmin_z5,
  cz.coospo_z1, cz.coospo_z2, cz.coospo_z3, cz.coospo_z4, cz.coospo_z5

from {{ ref('stg_sessions') }} s
left join garmin_hr gh on s.session_id = gh.session_id
left join coospo_hr ch on s.session_id = ch.session_id
left join garmin_z gz  on s.session_id = gz.session_id
left join coospo_z cz  on s.session_id = cz.session_id
