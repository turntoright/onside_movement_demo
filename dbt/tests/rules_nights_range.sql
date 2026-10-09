select * from {{ ref('standdown_rules') }} where nights < 1 or nights > 14
