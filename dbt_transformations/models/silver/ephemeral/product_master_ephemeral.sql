{{
  config(
    materialized = 'ephemeral',
    )
}}

select product_code,brand_name, therapy_area,launch_date,lifecycle_stage,
data_date, load_date
from {{ ref('product_master_bronze') }}
qualify row_number() over (partition by product_code,brand_name, therapy_area,launch_date,lifecycle_stage
order by data_date desc) = 1