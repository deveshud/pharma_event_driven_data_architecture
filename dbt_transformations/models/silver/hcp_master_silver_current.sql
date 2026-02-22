select * from {{ ref('hcp_master_silver') }}
where dbt_valid_to is NULL