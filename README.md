# Garmin vs Coospo HR Reconciliation (dbt)

This dbt project reconciles heart-rate measurements from two devices (Garmin watch vs Coospo chest strap)
by applying a single canonical zone system based on LTHR (Lactate Threshold HR).

## Inputs (must exist in your warehouse)
Schema: raw

- raw.garmin_hr_samples(session_id, sample_ts, hr_bpm, ...)
- raw.coospo_hr_samples(session_id, sample_ts, hr_bpm, ...)
- raw.sessions(session_id, sport, start_ts, end_ts, lthr_bpm, ...)

## Outputs
- fct_session_hr_summary: avg/max/p95 HR per session and device
- fct_session_time_in_zone: seconds and pct in each zone per session and device (canonical zones)
- rpt_garmin_vs_coospo_recon: side-by-side reconciliation view

## Run
dbt deps
dbt build
