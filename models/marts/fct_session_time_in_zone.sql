{% set bucket = var('hr_resample_seconds', 5) %}

with s as (
  select
    r.session_id,
    r.device,
    r.hr_bpm,
    sess.lthr_bpm
  from {{ ref('int_hr_samples_resampled') }} r
  join {{ ref('stg_sessions') }} sess using (session_id)
),

z as (
  select
    session_id,
    device,
    {{ hr_zone_from_lthr('hr_bpm', 'lthr_bpm') }} as zone,
    count(*) * {{ bucket }} as seconds_in_zone
  from s
  group by 1,2,3
),

tot as (
  select
    session_id,
    device,
    sum(seconds_in_zone) as total_seconds
  from z
  group by 1,2
),

final as (
  select
    z.session_id,
    z.device,
    z.zone,
    z.seconds_in_zone,
    case when t.total_seconds > 0 then z.seconds_in_zone * 1.0 / t.total_seconds else null end as pct_in_zone
  from z
  join tot t
    on z.session_id = t.session_id
   and z.device = t.device
)

select * from final
