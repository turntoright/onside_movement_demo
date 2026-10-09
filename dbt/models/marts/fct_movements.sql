-- Consecutive visits by the same person at different properties (arXiv 2602.21491;
-- spec §5). Timing uses visit_end, so inferred check-outs are allowed here.
with v as (
    select
        v.*,
        p.owner_company_id,
        p.lat,
        p.lon
    from {{ ref('int_visits') }} as v
    join {{ ref('stg_properties') }} as p on p.property_id = v.property_id
),

seq as (
    select
        *,
        lag(checkin_event_id) over w as prev_checkin_event_id,
        lag(property_id) over w as prev_property_id,
        lag(visit_end) over w as prev_visit_end,
        lag(owner_company_id) over w as prev_owner_company_id,
        lag(lat) over w as prev_lat,
        lag(lon) over w as prev_lon
    from v
    window w as (partition by person_id order by checkin_time, checkin_event_id)
),

moves as (
    select
        prev_checkin_event_id as origin_checkin_event_id,
        checkin_event_id as dest_checkin_event_id,
        person_id,
        prev_property_id as origin_property_id,
        property_id as dest_property_id,
        prev_visit_end as departure_time,
        checkin_time as arrival_time,
        greatest(0, epoch(checkin_time) - epoch(prev_visit_end)) / 3600.0 as gap_hours,
        greatest(0, datediff(
            'day',
            cast((prev_visit_end at time zone 'Pacific/Auckland') as date),
            cast((checkin_time at time zone 'Pacific/Auckland') as date)
        )) as nights_between,
        2 * 6371 * asin(sqrt(
            pow(sin(radians(lat - prev_lat) / 2), 2)
            + cos(radians(prev_lat)) * cos(radians(lat)) * pow(sin(radians(lon - prev_lon) / 2), 2)
        )) as distance_km,
        case when prev_owner_company_id = owner_company_id then 'internal' else 'external' end
            as scope,
        cast(date_trunc('month', checkin_time at time zone 'Pacific/Auckland') as date) as month
    from seq
    where prev_checkin_event_id is not null
      and prev_property_id <> property_id
)

select
    *,
    gap_hours < 24 * {{ var('connected_window_days') }} as connected
from moves
