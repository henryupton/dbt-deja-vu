{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        tags=['fp_mask']
    )
}}

{# Merge parent for the mask group; it stays unchanged after the group's initial. #}
select
    customer_id,
    customer_name,
    email,
    status,
    _updated_at,
    _loaded_at
from ({{ fp_customer_rows() }})
qualify row_number() over (partition by customer_id order by _updated_at desc) = 1
