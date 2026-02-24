with src as (
  select
    cast(session_id as {{ dbt.type_string() }}) as session_id,
    cast(sample_ts as {{ dbt.type_timestamp() }}) as sample_ts,
    cast(hr_bpm as integer) as hr_bpm
  from {{ source('raw', 'coospo_hr_samples') }}
),

final as (
  select
    session_id,
    sample_ts,
    hr_bpm,
    'coospo' as device
  from src
  where hr_bpm is not null
)

select * from final
