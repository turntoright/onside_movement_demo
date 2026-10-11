{% macro compacted_events() -%}
read_json('{{ env_var("DBT_COMPACTED_GLOB", "../data/sim/compacted/*/*.ndjson") }}',
    format = 'newline_delimited',
    columns = {
        'id': 'VARCHAR',
        'source': 'VARCHAR',
        'detail-type': 'VARCHAR',
        'time': 'VARCHAR',
        'detail': 'JSON'
    })
{%- endmacro %}

{% macro ref_file(name) -%}
read_json('{{ env_var("DBT_REF_ROOT", "../data/sim/ref/") }}{{ name }}.json', format = 'array')
{%- endmacro %}
