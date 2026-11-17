{{
    config(
        materialized='guarded_table',
        tags=['fp_sql']
    )
}}

{# Renders fp_sql_label into its SQL: a var-only change moves its output, not its checksum. #}
select customer_id, status, '{{ var("fp_sql_label", "base") }}' as label
from {{ ref('fp_sql_parent') }}
