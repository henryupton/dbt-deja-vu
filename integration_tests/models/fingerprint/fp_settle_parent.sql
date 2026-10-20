{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        pre_hook="{{ fp_settle_lose_verdict('park') }}",
        post_hook="{{ fp_settle_lose_verdict('restore') }}",
        tags=['fp_settle']
    )
}}

{# Merge parent for the settle group. fp_settle_flip lowers the status below the watermark; with fp_settle_lose #}
{# naming it, its hooks take the node ledger away for the length of its fingerprint post-hook. #}
select
    customer_id,
    customer_name,
    email,
    {% if var('fp_settle_flip', false) %}lower(status){% else %}status{% endif %} as status,
    _updated_at,
    _loaded_at
from ({{ fp_customer_rows() }})
qualify row_number() over (partition by customer_id order by _updated_at desc) = 1
