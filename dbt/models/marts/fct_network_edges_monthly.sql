-- Monthly directed edges; weight = average daily movements, as in the paper.
select
    month,
    origin_property_id,
    dest_property_id,
    count(*) as movements,
    count(*) / cast(day(last_day(month)) as double) as avg_daily_movements
from {{ ref('fct_movements') }}
group by month, origin_property_id, dest_property_id
