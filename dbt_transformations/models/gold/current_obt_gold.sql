{% set configs = [
    {
        "table_name": "rx_events_silver",
        "cols":"rx_events.rx_id,rx_events.rx_date,rx_events.product_code,rx_events.hcp_id,rx_events.quantity,rx_events.days_supply,rx_events.region,rx_events.payer_type,rx_events.channel",
        "alias":"rx_events"
    },
    {
        "table_name": "hcp_master_silver_current",
        "cols":"hcp_master.specialty,hcp_master.territory,hcp_master.engagement_tier",
        "alias":"hcp_master",
        "join_condition":"hcp_master.hcp_id = rx_events.hcp_id"
    },
    {
        "table_name": "product_master_silver_current",
        "cols":"product_master.brand_name,product_master.therapy_area,product_master.launch_date,product_master.lifecycle_stage",
        "alias":"product_master",
        "join_condition":"product_master.product_code = rx_events.product_code"
    }
] %}

select {% for config in configs %}
    {{ config.cols }}{% if not loop.last %},{% endif %}
    {% endfor %}
    ,{{audit_cols()}}
from {{ ref(configs[0].table_name) }} as {{ configs[0].alias }}
    {% for config in configs[1:] %}
        left join {{ ref(config.table_name) }} as {{ config.alias }}
        on {{ config.join_condition }}
{% endfor %}