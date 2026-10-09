-- Golden cases reshaped into the relations the standdown macro expects (spec §6.3).
select
    case_id as person_id,
    case_id || ':' || visit_id as checkin_event_id,
    case_id || ':' || property_id as property_id,
    cast(checkin_time as timestamptz) as checkin_time,
    case
        when checkout_time is null or checkout_time = '' then cast(checkin_time as timestamptz)
        else greatest(cast(checkin_time as timestamptz), cast(checkout_time as timestamptz))
    end as standdown_end,
    role,
    expected_breach,
    nullif(expected_rule_id, '') as expected_rule_id,
    case
        when expected_origin_visit_id is null or expected_origin_visit_id = '' then null
        else case_id || ':' || expected_origin_visit_id
    end as expected_origin_checkin_event_id
from {{ ref('golden_cases') }}
