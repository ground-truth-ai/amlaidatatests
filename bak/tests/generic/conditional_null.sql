{% test conditional_null(model, column_name, expression) %}

with validation_errors as (

    select
        1
    from {{ model }}
        where 
            {{ expression }} 
            AND {{ column_name }} is not null
)

select *
from validation_errors

{% endtest %}