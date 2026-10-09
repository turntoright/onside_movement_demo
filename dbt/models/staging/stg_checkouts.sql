with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'checkin_event_id' as checkin_event_id,
        detail ->> 'person_id' as person_id,
        detail ->> 'property_id' as property_id,
        cast(detail ->> 'event_time' as timestamptz) as checkout_time,
        cast(detail ->> 'received_time' as timestamptz) as received_time,
        detail ->> 'method' as method
    from {{ ref('stg_events') }}
    where detail_type = 'CheckOut'
)

select * from typed
qualify row_number() over (partition by event_id order by received_time) = 1
