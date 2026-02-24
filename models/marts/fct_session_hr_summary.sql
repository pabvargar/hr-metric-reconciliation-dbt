with samples as (
  select
    r.session_id,
    r.device,
    r.hr_bpm
  from {{ ref('int_hr_samples_resampled') }} r
),

agg as (
  select
    session_id,
    device,
    avg(hr_bpm) as avg_hr_bpm,
    max(hr_bpm) as max_hr_bpm,
    percentile_cont(0.95) within group (order by hr_bpm) as p95_hr_bpm,
    count(*) as buckets_observed
  from samples
  group by 1,2
)

select * from agg
