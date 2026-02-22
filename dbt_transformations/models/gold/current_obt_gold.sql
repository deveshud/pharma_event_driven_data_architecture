select rx_id,rx_date,a.product_code,a.hcp_id,
quantity,days_supply,region,payer_type,channel,
specialty,territory,engagement_tier,
brand_name,therapy_area,launch_date,lifecycle_stage,
{{audit_cols()}}
from {{ ref('rx_events_silver') }} as a 
    left join {{ ref('hcp_master_silver_current') }} as b
    on a.hcp_id = b.hcp_id
    left join {{ ref('product_master_silver_current') }} as c
    on a.product_code = c.product_code
