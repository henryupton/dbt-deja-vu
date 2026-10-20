{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        pre_hook="{{ fp_sql_legacy_row_hook() }}",
        tags=['fp_sql']
    )
}}

{# Merge parent for the compiled-SQL group; it stays unchanged after the group's initial. Its pre-hook can #}
{# plant a hashless 0.0.1 ledger row for a guarded child, before that child's guard runs. #}
select
    customer_id,
    customer_name,
    email,
    status,
    _updated_at,
    _loaded_at
from ({{ fp_customer_rows() }})
qualify row_number() over (partition by customer_id order by _updated_at desc) = 1
