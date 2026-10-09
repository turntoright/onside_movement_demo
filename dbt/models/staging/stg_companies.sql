select company_id, name, kind
from {{ ref_file('companies') }}
