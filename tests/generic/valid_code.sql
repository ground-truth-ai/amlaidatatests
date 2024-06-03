{% test valid_code(model, column_name, code_model) %}

with validation_errors as (

    select
        {{ column_name }}

    from {{ model }}

    where {{ column_name }} NOT IN (
        select code from {{ ref(code_model) }}
    )
)

select *
from validation_errors

{% endtest %}