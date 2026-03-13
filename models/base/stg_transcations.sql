{{ config(materialized='incremental', incremental_strategy='merge', unique_key='hash')}}

select
HASH,
BLOCK_NUMBER,
DATE,
FROM_ADDRESS,
TO_ADDRESS,
VALUE,
RECEIPT_CONTRACT_ADDRESS,
INPUT

from {{ source('eth', 'transactions')}}


{% if is_incremental() %}

where date >= (select max(date) from {{this}} )

{% endif %}
