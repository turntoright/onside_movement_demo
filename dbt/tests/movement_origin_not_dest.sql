select * from {{ ref('fct_movements') }} where origin_property_id = dest_property_id
