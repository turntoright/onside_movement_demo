-- All envelopes from compacted files; poison events never count (spec §5).
select
    source,
    "detail-type" as detail_type,
    detail
from {{ compacted_events() }}
where json_extract(detail, '$.poison') is null
