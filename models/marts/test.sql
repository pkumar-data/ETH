{{ config(group = 'fraud_risk') }}

select 
*
from {{ref('confirmed_fraud')}} 