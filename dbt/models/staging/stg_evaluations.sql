with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'checkin_event_id' as checkin_event_id,
        detail ->> 'person_id' as person_id,
        cast(detail ->> 'version_read' as integer) as version_read,
        cast(detail ->> 'version_committed' as integer) as version_committed,
        cast(detail ->> 'detected_time' as timestamptz) as detected_time,
        cast(detail ->> 'processed_at' as timestamptz) as processed_at,
        cast(detail ->> 'breach' as boolean) as breach,
        detail ->> 'rule_id' as rule_id,
        detail ->> 'origin_checkin_event_id' as origin_checkin_event_id,
        cast(detail ->> 'required_nights' as integer) as required_nights,
        cast(detail ->> 'nights_between' as integer) as nights_between,
        detail -> 'origins_seen' as origins_seen
    from {{ ref('stg_events') }}
    where detail_type = 'CheckInEvaluated'
)

select * from typed
qualify row_number() over (partition by checkin_event_id order by processed_at) = 1
