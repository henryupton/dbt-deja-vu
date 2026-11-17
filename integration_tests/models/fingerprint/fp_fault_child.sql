{{
    config(
        materialized='guarded_table',
        tags=['fp_fault']
    )
}}

{# The fault group's control: a plain guarded child of an unchanged parent, so it skips whenever the ledger works. #}
select customer_id, email, status
from {{ ref('fp_fault_parent') }}
