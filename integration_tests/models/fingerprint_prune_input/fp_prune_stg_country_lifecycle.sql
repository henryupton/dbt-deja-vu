{{
    config(
        tags=['fp_prune_input']
    )
}}

{# Frozen staging for the country dim; every row is at stage 0, so it never moves. #}
select country_code, country_name, region, _updated_at, _loaded_at
from {{ ref('fp_prune_raw_country_lifecycle') }}
where _stage <= {{ var('fp_prune_stage', 0) | int }}
