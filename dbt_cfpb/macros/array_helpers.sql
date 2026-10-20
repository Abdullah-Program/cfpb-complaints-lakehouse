{% macro array_contains_val(array_col, val) -%}
    {%- if target.name == 'snowflake' -%}
        array_contains('{{ val }}'::variant, {{ array_col }})
    {%- else -%}
        list_contains({{ array_col }}, '{{ val }}')
    {%- endif -%}
{%- endmacro %}
