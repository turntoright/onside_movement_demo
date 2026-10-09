-- One row per check-in. standdown_end ignores inferred check-outs (spec §4.1);
-- visit_end allows them (spec §5) and drives movements and tracing.
with checkins as (
    select * from {{ ref('stg_checkins') }}
),

checkouts as (
    select checkin_event_id, min(checkout_time) as checkout_time
    from {{ ref('stg_checkouts') }}
    group by checkin_event_id
),

joined as (
    select
        ci.event_id as checkin_event_id,
        ci.person_id,
        ci.property_id,
        ci.checkin_time,
        ci.received_time,
        co.checkout_time,
        lead(ci.checkin_time) over (
            partition by ci.person_id order by ci.checkin_time, ci.event_id
        ) as next_checkin_time,
        (
            cast((ci.checkin_time at time zone 'Pacific/Auckland') as date)
            + interval 1 day - interval 1 second
        ) at time zone 'Pacific/Auckland' as end_of_day
    from checkins as ci
    left join checkouts as co on co.checkin_event_id = ci.event_id
)

select
    checkin_event_id,
    person_id,
    property_id,
    checkin_time,
    received_time,
    checkout_time,
    case
        when checkout_time is null then checkin_time
        else greatest(checkin_time, checkout_time)
    end as standdown_end,
    case
        when checkout_time is not null then checkout_time
        when next_checkin_time is not null and next_checkin_time < end_of_day then next_checkin_time
        else end_of_day
    end as visit_end,
    case
        when checkout_time is not null then 'actual'
        when next_checkin_time is not null and next_checkin_time < end_of_day
            then 'inferred_next_checkin'
        else 'inferred_end_of_day'
    end as checkout_source
from joined
