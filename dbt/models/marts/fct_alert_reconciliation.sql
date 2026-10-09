-- Spec §6.2: realtime vs batch, with a provable cause for every miss.
with batch as (
    select * from {{ ref('fct_high_risk_visits') }}
),

evaluations as (
    select * from {{ ref('stg_evaluations') }}
),

checkins as (
    select event_id, checkin_time, received_time from {{ ref('stg_checkins') }}
),

checkout_versions as (
    select checkin_event_id, min(version_committed) as version_committed
    from {{ ref('stg_checkout_applied') }}
    group by checkin_event_id
),

keys as (
    select checkin_event_id from batch
    union
    select checkin_event_id from evaluations where breach
),

joined as (
    select
        k.checkin_event_id,
        b.checkin_event_id is not null as batch_breach,
        coalesce(e.breach, false) as realtime_breach,
        e.checkin_event_id is not null as evaluated,
        e.version_read,
        b.origin_checkin_event_id as batch_origin_checkin_event_id,
        b.rule_id as batch_rule_id,
        e.origin_checkin_event_id as realtime_origin_checkin_event_id,
        e.rule_id as realtime_rule_id
    from keys as k
    left join batch as b on b.checkin_event_id = k.checkin_event_id
    left join evaluations as e on e.checkin_event_id = k.checkin_event_id
),

evidence as (
    select
        j.*,
        os.origin_checkin_event_id is not null as origin_seen,
        os.end_source as origin_end_source,
        oe.version_committed as origin_checkin_version,
        oc.version_committed as origin_checkout_version,
        d.checkin_time,
        d.received_time,
        o.received_time as origin_received_time
    from joined as j
    left join {{ ref('stg_evaluation_origins') }} as os
        on os.target_checkin_event_id = j.checkin_event_id
        and os.origin_checkin_event_id = j.batch_origin_checkin_event_id
    left join evaluations as oe on oe.checkin_event_id = j.batch_origin_checkin_event_id
    left join checkout_versions as oc on oc.checkin_event_id = j.batch_origin_checkin_event_id
    left join checkins as d on d.event_id = j.checkin_event_id
    left join checkins as o on o.event_id = j.batch_origin_checkin_event_id
),

classified as (
    select
        *,
        case
            when batch_breach and realtime_breach then 'matched'
            when realtime_breach then 'false_alert'
            else 'missed'
        end as status,
        case
            when realtime_breach then null
            when not evaluated then 'not_evaluated'
            when not origin_seen and origin_checkin_version > version_read then 'origin_not_seen'
            when origin_seen and origin_end_source = 'checkin'
                and origin_checkout_version > version_read then 'origin_end_not_seen'
            else 'unexplained'
        end as cause
    from evidence
)

select
    checkin_event_id,
    status,
    cause,
    case
        when cause = 'origin_not_seen' and origin_received_time > received_time then 'offline'
        when cause = 'origin_not_seen' then 'reordered'
    end as sub_cause,
    case
        when status = 'matched'
            then batch_rule_id = realtime_rule_id
            and batch_origin_checkin_event_id = realtime_origin_checkin_event_id
    end as same_attribution,
    case
        when status = 'matched' then epoch(received_time) - epoch(checkin_time) > 3600
    end as late_alert,
    batch_origin_checkin_event_id,
    batch_rule_id,
    realtime_origin_checkin_event_id,
    realtime_rule_id,
    version_read,
    origin_checkin_version,
    origin_checkout_version
from classified
