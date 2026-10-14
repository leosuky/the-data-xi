{# Reference a column that only exists in the relation once a provider starts
   shipping it (e.g. physical tracking, 2026/27+). Emits alias.col cast to
   numeric when present in `cols`, else a typed NULL, so the model compiles on
   databases loaded before the column appeared. #}
{% macro col_or_null(cols, alias, col) -%}
    {%- if col in cols -%} {{ alias }}.{{ col }}::numeric {%- else -%} null::numeric {%- endif -%}
{%- endmacro %}
