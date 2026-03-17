select 
date,
transaction_category,
count(*) as tx_count,
sum(value)/1e18 as sum_value_ethereum
from {{ ref('transcations_enriched')}}
group by date, transaction_category