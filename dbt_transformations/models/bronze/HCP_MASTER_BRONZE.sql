{{
  config(
    materialized = 'incremental',
    )
}}
select *,
to_date(load_ts) as load_date,
to_date(
  regexp_substr(src_file, 'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1),
  'YYYY-MM-DD'
) as data_date,
{{ audit_cols() }}
from
{{source('raw', 'hcp_master_raw')}}

{% if is_incremental() %}
  where to_date(load_ts) >= coalesce((select max(to_date(load_ts)) from {{ this }}), '1900-01-01')
{% endif %} 