select *,to_date(load_ts) from {{ source('raw', 'hcp_master_raw') }}


 {# >= coalesce((select max(to_date(load_ts)) from {{ ref('hcp_master_bronze') }}), '1900-01-01') #}