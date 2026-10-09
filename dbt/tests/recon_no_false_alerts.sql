-- Spec §4: realtime can only miss, never false-alert.
select * from {{ ref('fct_alert_reconciliation') }} where status = 'false_alert'
