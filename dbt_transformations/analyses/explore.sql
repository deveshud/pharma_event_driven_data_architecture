{# select * from {{ source('raw', 'hcp_master_raw') }} #}

select * from {{ ref('hcp_master_bronze') }}
order by hcp_id, load_ts desc