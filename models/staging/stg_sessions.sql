with src as (
  select
    cast(session_id as {{ dbt.type_string() }}) as session_id,
    cast(sport as {{ dbt.type_string() }}) as sport,
    cast(start_ts as {{ dbt.type_timestamp() }}) as start_ts,
    cast(end_ts as {{ dbt.type_timestamp() }}) as end_ts,
    cast(lthr_bpm as integer) as lthr_bpm
  from {{ source('raw', 'sessions') }}
)

select * from src
