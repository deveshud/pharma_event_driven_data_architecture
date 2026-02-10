{% macro audit_cols(pipeline_name = 'dbt_model') %}
    current_date() as created_at,
    current_user() as created_by,
    '{{ invocation_id }}' as load_id,
    '{{ target.name }}' as environment,
    '{{ pipeline_name }}' as pipeline_name
{% endmacro %}