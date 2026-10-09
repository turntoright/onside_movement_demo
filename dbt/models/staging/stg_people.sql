select person_id, visitor_company_id, role, home_region
from {{ ref_file('people') }}
