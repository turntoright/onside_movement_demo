with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'checkin_event_id' as checkin_event_id,
        detail ->> 'person_id' as person_id,
        detail ->> 'origin_property_id' as origin_property_id,
        detail ->> 'dest_property_id' as dest_property_id,
        detail ->> 'origin_checkin_event_id' as origin_checkin_event_id,
        cast(detail ->> 'nights_between' as integer) as nights_between,
        cast(detail ->> 'required_nights' as integer) as required_nights,
        detail ->> 'rule_id' as rule_id,
        cast(detail ->> 'detected_time' as timestamptz) as detected_time,
        cast(detail ->> 'processed_at' as timestamptz) as processed_at
    from {{ ref('stg_events') }}
    where detail_type = 'HighRiskVisitDetected'
)

select * from typed
qualify row_number() over (partition by checkin_event_id order by processed_at) = 1
