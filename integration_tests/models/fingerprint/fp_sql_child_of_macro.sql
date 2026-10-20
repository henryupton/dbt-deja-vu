{{
    config(
        materialized='guarded_table',
        tags=['fp_sql']
    )
}}

{# Renders a macro the runner rewrites (macros/fp_sql_expr.sql): its output moves, its checksum does not. #}
select customer_id, {{ fp_sql_status_expr('status') }} as status
from {{ ref('fp_sql_parent') }}
