select
    sum(value) as total_amount
from {{ ref('transcations_enriched') }}

having total_amount < 0
