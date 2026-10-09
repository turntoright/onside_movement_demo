select 'checkin_without_evaluation' as problem, c.event_id
from {{ ref('stg_checkins') }} as c
left join {{ ref('stg_evaluations') }} as e on e.checkin_event_id = c.event_id
where e.checkin_event_id is null
union all
select 'checkout_without_applied' as problem, o.event_id
from {{ ref('stg_checkouts') }} as o
left join {{ ref('stg_checkout_applied') }} as a on a.checkout_event_id = o.event_id
where a.checkout_event_id is null
