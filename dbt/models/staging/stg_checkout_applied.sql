with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'checkout_event_id' as checkout_event_id,
        detail ->> 'checkin_event_id' as checkin_event_id,
        detail ->> 'person_id' as person_id,
        cast(detail ->> 'version_committed' as integer) as version_committed,
        cast(detail ->> 'processed_at' as timestamptz) as processed_at
    from {{ ref('stg_events') }}
    where detail_type = 'CheckOutApplied'
)

select * from typed
qualify row_number() over (partition by checkout_event_id order by processed_at) = 1
