-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%} {# Default schema from profiles.yml #}

    {%- if target.name == 'stg' -%}
        {# For the 'stg' target, append '_stg' #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}_stg
        {%- endif -%}
    {%- elif target.name == 'prod' -%}
        {# For the 'prod' target, use names as is #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- else -%}
        {# Fallback for any other targets (e.g., 'dev') - behaves like prod for custom names, else default schema #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}