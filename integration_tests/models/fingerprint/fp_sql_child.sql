{{
    config(
        materialized='guarded_table',
        tags=['fp_sql']
    )
}}

{# The compiled-SQL group's control: nothing it renders moves, so it skips whenever its parent is unchanged. #}
select customer_id, email, status
from {{ ref('fp_sql_parent') }}
