-- Spec §3.3: check-in <= check-out <= check-in + 48 hours.
select o.event_id, c.checkin_time, o.checkout_time
from {{ ref('stg_checkouts') }} as o
join {{ ref('stg_checkins') }} as c on c.event_id = o.checkin_event_id
where o.checkout_time < c.checkin_time
   or o.checkout_time > c.checkin_time + interval 48 hours
