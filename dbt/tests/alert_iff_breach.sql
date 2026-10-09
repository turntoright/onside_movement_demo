select e.checkin_event_id as evaluated_id, a.checkin_event_id as alert_id, e.breach
from {{ ref('stg_evaluations') }} as e
full outer join {{ ref('stg_realtime_alerts') }} as a on a.checkin_event_id = e.checkin_event_id
where coalesce(e.breach, false) <> (a.checkin_event_id is not null)
