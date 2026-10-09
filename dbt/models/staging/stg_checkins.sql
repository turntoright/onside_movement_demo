with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'person_id' as person_id,
        detail ->> 'visitor_company_id' as visitor_company_id,
        detail ->> 'property_id' as property_id,
        cast(detail ->> 'event_time' as timestamptz) as checkin_time,
        cast(detail ->> 'received_time' as timestamptz) as received_time,
        detail ->> 'method' as method,
        detail ->> 'job_type' as job_type,
        cast(detail ->> 'additional_visitors' as integer) as additional_visitors
    from {{ ref('stg_events') }}
    where detail_type = 'CheckIn'
)

select * from typed
qualify row_number() over (partition by event_id order by received_time) = 1
