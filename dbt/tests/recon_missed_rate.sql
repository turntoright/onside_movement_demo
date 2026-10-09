{{ config(severity='warn') }}
with s as (
    select
        count(*) filter (where status = 'missed') as missed,
        count(*) filter (where status in ('matched', 'missed')) as batch_breaches
    from {{ ref('fct_alert_reconciliation') }}
)
select * from s
where batch_breaches > 0
  and missed / cast(batch_breaches as double) > {{ var('missed_rate_warn') }}
