{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append'
  )
}}

select
  rx_id,
  rx_date,
  product_code,
  hcp_id,
  quantity,
  days_supply,
  region,
  payer_type,
  channel,
  load_date,
  data_date,
  load_ts,
  {{ audit_cols() }}
from {{ ref('rx_events_bronze') }}

{% if is_incremental() %}
    where load_ts >
    (
        select max(load_ts)
        from {{ this }}
    )
{% endif %}