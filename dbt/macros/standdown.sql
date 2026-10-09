{#
  Spec §4: one row per breaching check-in.
  visits:     person_id, checkin_event_id, property_id, checkin_time, standdown_end
  properties: property_id, property_type, owner_company_id
  rules:      rule_id, from_type, to_type, scope, nights
#}
{% macro standdown(visits, properties, rules) -%}
with v as (
    select
        v.person_id,
        v.checkin_event_id,
        v.property_id,
        v.checkin_time,
        v.standdown_end,
        p.property_type,
        p.owner_company_id
    from {{ visits }} as v
    join {{ properties }} as p on p.property_id = v.property_id
),

pairs as (
    select
        d.person_id,
        d.checkin_event_id as target_id,
        o.checkin_event_id as origin_id,
        o.property_id as origin_property_id,
        d.property_id as dest_property_id,
        o.standdown_end as origin_end,
        greatest(0, datediff(
            'day',
            cast((o.standdown_end at time zone 'Pacific/Auckland') as date),
            cast((d.checkin_time at time zone 'Pacific/Auckland') as date)
        )) as nights_between,
        o.property_type as from_type,
        d.property_type as to_type,
        case when o.owner_company_id = d.owner_company_id then 'internal' else 'external' end
            as pair_scope
    from v as d
    join v as o
        on o.person_id = d.person_id
        and (
            o.checkin_time < d.checkin_time
            or (o.checkin_time = d.checkin_time and o.checkin_event_id < d.checkin_event_id)
        )
        and o.property_id <> d.property_id
),

ruled as (
    select
        pairs.*,
        r.rule_id,
        r.nights as required_nights,
        row_number() over (
            partition by pairs.target_id, pairs.origin_id
            order by case when r.scope = pairs.pair_scope then 0 else 1 end
        ) as rule_rank
    from pairs
    join {{ rules }} as r
        on r.from_type = pairs.from_type
        and r.to_type = pairs.to_type
        and (r.scope = pairs.pair_scope or r.scope = 'any')
    where pairs.nights_between <= 14
),

breaches as (
    select * from ruled
    where rule_rank = 1 and nights_between < required_nights
),

picked as (
    select
        *,
        row_number() over (
            partition by target_id
            order by
                required_nights desc,
                required_nights - nights_between desc,
                origin_end desc,
                origin_id desc
        ) as pick
    from breaches
)

select
    person_id,
    target_id as checkin_event_id,
    origin_id as origin_checkin_event_id,
    origin_property_id,
    dest_property_id,
    rule_id,
    required_nights,
    nights_between
from picked
where pick = 1
{%- endmacro %}
