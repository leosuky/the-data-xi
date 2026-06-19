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
