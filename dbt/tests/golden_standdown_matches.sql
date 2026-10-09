-- The SQL stand-down logic must agree with rules.py on every golden case.
with actual as (
    {{ standdown(ref('stg_golden_visits'), ref('stg_golden_properties'), ref('standdown_rules')) }}
),

targets as (
    select * from {{ ref('stg_golden_visits') }} where role = 'target'
)

select
    t.checkin_event_id,
    t.expected_breach,
    t.expected_rule_id,
    t.expected_origin_checkin_event_id,
    a.rule_id,
    a.origin_checkin_event_id
from targets as t
left join actual as a on a.checkin_event_id = t.checkin_event_id
where (a.checkin_event_id is not null) <> t.expected_breach
   or coalesce(a.rule_id, '') <> coalesce(t.expected_rule_id, '')
   or coalesce(a.origin_checkin_event_id, '') <> coalesce(t.expected_origin_checkin_event_id, '')
