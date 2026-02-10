select *,
to_date(load_ts) as data_date,
{{ audit_cols() }}
from
{{source('RAW', 'HCP_MASTER_RAW')}}
