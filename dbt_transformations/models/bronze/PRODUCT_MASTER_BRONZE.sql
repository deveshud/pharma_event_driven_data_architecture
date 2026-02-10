select *,
to_date(load_ts) as data_date,
{{ audit_cols() }}
from
{{source('RAW', 'PRODUCT_MASTER_RAW')}}
