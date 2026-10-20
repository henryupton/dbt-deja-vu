{#
  Test-only: the as-of join predicate fp_prune_fct_daily renders, and the only model that does. The runner rewrites
  this file from scenario 93 with the same predicate written the other way round, so the fact's compiled SQL moves
  while its output and its own checksum stay put.
#}
{% macro fp_prune_as_of_join(fact_date, dim) %}
  {{ return(fact_date ~ ' >= ' ~ dim ~ '._valid_from::date and ' ~ fact_date ~ ' < ' ~ dim ~ '._valid_to::date') }}
{% endmacro %}
