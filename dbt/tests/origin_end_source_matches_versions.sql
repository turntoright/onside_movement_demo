-- An origin's end_source is 'checkout' exactly when its check-out was applied before the read.
select os.*, e.version_read, oa.version_committed
from {{ ref('stg_evaluation_origins') }} as os
join {{ ref('stg_evaluations') }} as e on e.checkin_event_id = os.target_checkin_event_id
left join (
    select checkin_event_id, min(version_committed) as version_committed
    from {{ ref('stg_checkout_applied') }}
    group by checkin_event_id
) as oa on oa.checkin_event_id = os.origin_checkin_event_id
where (os.end_source = 'checkout') <> coalesce(oa.version_committed <= e.version_read, false)
