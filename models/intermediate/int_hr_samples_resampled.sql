{% set bucket = var('hr_resample_seconds', 5) %}

with samples as (
  select
    s.session_id,
    s.device,
    s.sample_ts,
    s.hr_bpm
  from {{ ref('int_hr_samples_union') }} s
),

bucketed as (
  select
    session_id,
    device,
    -- Create integer epoch bucket to be adapter-agnostic
    cast(
      floor(
        {{ dbt_utils.date_diff("timestamp '1970-01-01'", "sample_ts", "second") }} / {{ bucket }}
      ) as bigint
    ) as bucket_id,
    max(hr_bpm) as hr_bpm_bucket
  from samples
  group by 1,2,3
),

final as (
  select
    session_id,
    device,
    bucket_id,
    hr_bpm_bucket as hr_bpm
  from bucketed
)

select * from final
