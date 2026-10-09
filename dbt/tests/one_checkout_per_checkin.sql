-- Spec §3.3: at most one distinct check-out per check-in.
select checkin_event_id, count(*) as checkouts
from {{ ref('stg_checkouts') }}
group by checkin_event_id
having count(*) > 1
