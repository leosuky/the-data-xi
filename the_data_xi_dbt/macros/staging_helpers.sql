{# ============================================================================
   Staging helper macros for The Data XI.
   ========================================================================= #}

{# Normalize a player/team name for cross-provider matching:
   lowercase, strip everything but a-z. NOTE: install the `unaccent`
   extension and wrap with unaccent() for accented names (Kvaratskhelia,
   Mller) - without it, accents are simply dropped, which is usually fine
   but can collide rare names. #}
{% macro normalize_name(col) -%}
    regexp_replace(lower({{ col }}), '[^a-z]', '', 'g')
{%- endmacro %}

{# Extract the leading number from a Fotmob-style display string:
   '67%' -> 67 ,  '12/20 (60%)' -> 12 ,  '1.8' -> 1.8 ,  '-'/''/NULL -> NULL.
   Keeps non-numeric values as NULL rather than erroring. #}
{% macro extract_leading_number(col) -%}
    case when {{ col }} ~ '[0-9]'
         then regexp_replace({{ col }}, '^[^0-9.-]*([0-9]+\.?[0-9]*).*$', '\1')::numeric
    end
{%- endmacro %}

{# Dynamic TEXT->numeric cast for the auto-widened reference tables
   (sofa_match_stats, fot_team_stats, sofa_player_stats, fot_player_stats).
   Introspects the live relation so it survives season-varying columns.
     keep_text : columns to leave as TEXT (ids stored as text, labels)
     drop      : columns to omit entirely
     display   : for these, ALSO emit a "<col>_display" passthrough of the raw
                 string (use for Fotmob format strings you want to eyeball). #}
{% macro cast_text_numeric(relation, keep_text=[], drop=[], display=[]) -%}
    {%- set cols = adapter.get_columns_in_relation(relation) -%}
    {%- set out = [] -%}
    {%- for c in cols -%}
        {%- if c.name in drop -%}
        {%- elif c.name in keep_text or c.data_type not in ['text', 'character varying'] -%}
            {%- do out.append('"' ~ c.name ~ '"') -%}
        {%- else -%}
            {%- if c.name in display -%}
                {%- do out.append('"' ~ c.name ~ '" as "' ~ c.name ~ '_display"') -%}
            {%- endif -%}
            {%- do out.append(extract_leading_number('"' ~ c.name ~ '"') ~ ' as "' ~ c.name ~ '"') -%}
        {%- endif -%}
    {%- endfor -%}
    {{ out | join(',\n    ') }}
{%- endmacro %}

{# 1:1 passthrough: introspect the LIVE relation and emit every column
   (alias-qualified), minus `exclude`. Robust to auto-ALTER columns added at
   load time - a hardcoded column list silently drops those. #}
{% macro passthrough_columns(relation, alias, exclude=[]) -%}
    {%- set cols = adapter.get_columns_in_relation(relation) -%}
    {%- set out = [] -%}
    {%- for c in cols -%}
        {%- if c.name not in exclude -%}
            {%- do out.append(alias ~ '."' ~ c.name ~ '"') -%}
        {%- endif -%}
    {%- endfor -%}
    {{ out | join(',\n    ') }}
{%- endmacro %}

{# Dynamic long->wide pivot. Reads the distinct key values at compile time and
   emits one MAX(CASE ...) column each. value_col is cast to value_cast. #}
{% macro pivot_long_to_wide(relation, group_by, key_col, value_col, value_cast='numeric') -%}
    {%- if execute -%}
        {%- set keys = run_query('select distinct ' ~ key_col ~ ' from ' ~ relation ~
                                 ' where ' ~ key_col ~ ' is not null order by 1').columns[0].values() -%}
    {%- else -%}
        {%- set keys = [] -%}
    {%- endif -%}
    {%- set sel = [] -%}
    {%- for g in group_by -%}{%- do sel.append(g) -%}{%- endfor -%}
    {%- for k in keys -%}
        {%- set colname = k | lower | replace('.', '_') | replace(' ', '_') | replace('-', '_') | replace('+', 'plus') -%}
        {%- do sel.append("max(case when " ~ key_col ~ " = '" ~ k ~ "' then " ~ value_col ~ "::" ~ value_cast ~ " end) as \"" ~ colname ~ "\"") -%}
    {%- endfor -%}
    select
    {{ sel | join(',\n    ') }}
    from {{ relation }}
    group by {{ group_by | join(', ') }}
{%- endmacro %}

{# Emit only the columns of `rel` that are NOT already in `base_rel`
   (alias-qualified). For base+advanced fact joins, so b.* plus the advanced
   table's unique columns never collide. Assumes rel has >=1 unique column. #}
{% macro select_new_columns(rel, base_rel, alias) -%}
    {%- set rel_cols  = adapter.get_columns_in_relation(rel)      | map(attribute='name') | list -%}
    {%- set base_cols = adapter.get_columns_in_relation(base_rel) | map(attribute='name') | list -%}
    {%- set out = [] -%}
    {%- for c in rel_cols if c not in base_cols -%}
        {%- do out.append(alias ~ '."' ~ c ~ '"') -%}
    {%- endfor -%}
    {{ out | join(',\n    ') }}
{%- endmacro %}
