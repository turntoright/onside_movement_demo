with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'source_property_id' as source_property_id,
        detail ->> 'dest_property_id' as dest_property_id,
        cast(detail ->> 'movement_date' as date) as movement_date,
        detail ->> 'material_type' as material_type,
        cast(detail ->> 'volume' as integer) as volume,
        cast(detail ->> 'received_time' as timestamptz) as received_time
    from {{ ref('stg_events') }}
    where detail_type = 'PlantMovement'
)

select * from typed
qualify row_number() over (partition by event_id order by received_time) = 1
