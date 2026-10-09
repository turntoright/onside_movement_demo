-- One row per origin the Lambda saw when it evaluated a check-in (spec §4 bound).
with exploded as (
    select
        checkin_event_id as target_checkin_event_id,
        unnest(from_json(
            origins_seen,
            '[{"checkin_event_id": "VARCHAR", "end": "VARCHAR", "end_source": "VARCHAR", "nights_between": "INTEGER"}]'
        )) as origin
    from {{ ref('stg_evaluations') }}
)

select
    target_checkin_event_id,
    struct_extract(origin, 'checkin_event_id') as origin_checkin_event_id,
    cast(struct_extract(origin, 'end') as timestamptz) as origin_end,
    struct_extract(origin, 'end_source') as end_source,
    struct_extract(origin, 'nights_between') as nights_between
from exploded
