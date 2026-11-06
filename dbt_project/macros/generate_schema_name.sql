{#
    Override of dbt's default schema naming.

    THE PROBLEM THIS SOLVES
    By default dbt concatenates: a model configured with `schema: marts`, run
    against a target whose schema is STAGING, lands in a schema called
    `STAGING_marts`. You would end up with STAGING_marts and STAGING_staging
    instead of the MARTS and STAGING schemas created in Phase 3, and the
    permission error that follows is genuinely confusing to diagnose.

    WHAT THIS DOES INSTEAD
    Uses the configured schema name exactly as written, so `schema: marts`
    means MARTS. Snowflake upper-cases unquoted identifiers, so lowercase here
    resolves to the uppercase schema that already exists.

    WHY dev AND prod ARE SPECIAL-CASED
    Any other target gets its name prefixed, which is what isolates CI. A pull
    request running against the `ci` target builds CI_STAGING and CI_MARTS and
    cannot overwrite anything you are working on. That is what the
    `create schema on database` grant in Phase 3 is for.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- set chosen_schema = default_schema if custom_schema_name is none
                            else custom_schema_name | trim -%}

    {%- if target.name in ('dev', 'prod') -%}
        {{ chosen_schema }}
    {%- else -%}
        {{ target.name | upper }}_{{ chosen_schema }}
    {%- endif -%}

{%- endmacro %}
