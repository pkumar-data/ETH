{{ config(materialized='incremental', incremental_strategy='append')}}


with token_transfer_aggs as 
(select
transaction_hash,
count(*) as token_transfer_count
from {{ ref('stg_token_transfers')}}
group by transaction_hash
),


transcations_enriched AS (
select 
t.HASH,
t.BLOCK_NUMBER,
t.DATE,
t.FROM_ADDRESS,
t.TO_ADDRESS,
t.VALUE,
t.RECEIPT_CONTRACT_ADDRESS,
t.INPUT,
tt.token_transfer_count,
1 as field_1,

case
    when t.RECEIPT_CONTRACT_ADDRESS != '' then 'contract_creation'
    when tt.transaction_hash is not null then 'token_transfer'
    when t.INPUT = '0x' and t.VALUE > 0 then 'plain_eth_transfer'
    else 'other'
end as transaction_category

from {{ ref('stg_transcations')}} t


left join token_transfer_aggs tt 

on t.HASH = tt.transaction_hash


{% if is_incremental() %}

where date >= (select max(date) from {{this}} )

{% endif %} 

)

select * from transcations_enriched
