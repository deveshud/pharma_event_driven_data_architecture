{{
  config(
    materialized = 'ephemeral'
    )
}}

select hcp_id, specialty,territory,engagement_tier,load_date,data_date
from {{ ref('hcp_master_bronze') }}
qualify row_number() over (partition by hcp_id, specialty,territory,engagement_tier 
    order by data_date desc) = 1
