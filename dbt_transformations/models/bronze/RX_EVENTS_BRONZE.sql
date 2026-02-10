select *,
to_date(load_ts) as data_date,
{{ audit_cols() }}
from
{{source('RAW', 'RX_EVENTS_RAW')}}
