select sum(pagerank) as total
from {{ ref('dim_property_network') }}
having abs(sum(pagerank) - 1) > 1e-6
