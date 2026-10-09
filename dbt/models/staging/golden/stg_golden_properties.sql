select distinct
    case_id || ':' || property_id as property_id,
    property_type,
    owner_company_id
from {{ ref('golden_cases') }}
