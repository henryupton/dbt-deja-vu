{{
    config(
        materialized='guarded_table',
        pre_hook="{{ fp_settle_lose_verdict('park') }}",
        post_hook="{{ fp_settle_lose_verdict('restore') }}",
        tags=['fp_settle']
    )
}}

{# Guarded child of fp_settle_parent. It renders fp_settle_label, so that var moves its output and compiled SQL #}
{# but not its checksum; with fp_settle_lose naming it, its own verdict is lost. #}
select customer_id, status, '{{ var("fp_settle_label", "base") }}' as label
from {{ ref('fp_settle_parent') }}
