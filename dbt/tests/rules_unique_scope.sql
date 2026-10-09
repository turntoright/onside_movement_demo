select from_type, to_type, scope, count(*) as n
from {{ ref('standdown_rules') }}
group by from_type, to_type, scope
having count(*) > 1
