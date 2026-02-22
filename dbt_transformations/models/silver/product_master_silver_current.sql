select * from {{ ref('product_master_silver') }}
where dbt_valid_to is NULL