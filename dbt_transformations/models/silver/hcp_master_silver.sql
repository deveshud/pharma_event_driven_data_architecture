SELECT * FROM {{ ref('hcp_master_bronze') }}
WHERE DATA_DATE = '2026-02-08'