select * from {{ ref('fct_alert_reconciliation') }}
where cause in ('not_evaluated', 'unexplained')
