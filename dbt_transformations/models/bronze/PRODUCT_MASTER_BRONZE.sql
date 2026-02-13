select *,
to_date(load_ts) as data_date,
{{ audit_cols() }}
from
{{source('raw', 'product_master_raw')}}
