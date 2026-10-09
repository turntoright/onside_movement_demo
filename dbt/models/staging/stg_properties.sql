select
    property_id,
    name,
    property_type,
    group_id,
    owner_company_id,
    region,
    cast(lat as double) as lat,
    cast(lon as double) as lon
from {{ ref_file('properties') }}
