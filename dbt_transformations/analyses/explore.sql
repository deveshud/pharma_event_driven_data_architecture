select *, to_date(load_ts) from {{ source('raw', 'hcp_master_raw') }}
where to_date(load_ts) > (select max(to_date(load_ts)) from {{ ref('hcp_master_bronze') }} )