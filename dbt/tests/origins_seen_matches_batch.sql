-- Spec §6.3: origins_seen = batch candidates within 16 local days whose own check-in was
-- committed at or before the version the Lambda read.
with ev as (
    select * from {{ ref('stg_evaluations') }}
),

ci as (
    select event_id, person_id, property_id, checkin_time from {{ ref('stg_checkins') }}
),

d as (
    select ev.checkin_event_id, ev.version_read, ci.person_id, ci.property_id, ci.checkin_time
    from ev
    join ci on ci.event_id = ev.checkin_event_id
),

expected as (
    select d.checkin_event_id as target_checkin_event_id, o.event_id as origin_checkin_event_id
    from d
    join ci as o
        on o.person_id = d.person_id
        and (
            o.checkin_time < d.checkin_time
            or (o.checkin_time = d.checkin_time and o.event_id < d.checkin_event_id)
        )
        and o.property_id <> d.property_id
        and datediff(
            'day',
            cast((o.checkin_time at time zone 'Pacific/Auckland') as date),
            cast((d.checkin_time at time zone 'Pacific/Auckland') as date)
        ) <= 16
    join ev as oe
        on oe.checkin_event_id = o.event_id
        and oe.version_committed <= d.version_read
),

actual as (
    select target_checkin_event_id, origin_checkin_event_id
    from {{ ref('stg_evaluation_origins') }}
),

missing as (
    select * from expected
    except
    select * from actual
),

unexpected as (
    select * from actual
    except
    select * from expected
)

select 'missing' as problem, * from missing
union all
select 'unexpected' as problem, * from unexpected
