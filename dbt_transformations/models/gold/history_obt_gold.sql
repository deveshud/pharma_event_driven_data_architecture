select rx_id,rx_date,a.product_code,a.hcp_id,
quantity,days_supply,region,payer_type,channel,
specialty,territory,engagement_tier,
to_date(b.dbt_valid_from) as hcp_start_date,
to_date(b.dbt_valid_to) as hcp_end_date,
brand_name,therapy_area,launch_date,lifecycle_stage,
to_date(c.dbt_valid_from) as product_start_date,
to_date(c.dbt_valid_to) as product_end_date,
{{audit_cols()}}
from {{ ref('rx_events_silver') }} as a 
    left join {{ ref('hcp_master_silver_current') }} as b
    on a.hcp_id = b.hcp_id
    left join {{ ref('product_master_silver_current') }} as c
    on a.product_code = c.product_code
