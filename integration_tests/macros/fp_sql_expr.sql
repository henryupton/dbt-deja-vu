{#
  Test-only: the status expression fp_sql_child_of_macro renders. The runner rewrites this file in scenario 66,
  so the child's compiled SQL and output move while its own file, and so its checksum, stay put.
#}
{% macro fp_sql_status_expr(column) %}
  {{ return('upper(' ~ column ~ ')') }}
{% endmacro %}
