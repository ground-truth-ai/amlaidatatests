{% test single_value(model, column_name) %}

with validation as (

    select
        count(distinct {{ column_name }}) as count

    from {{ model }}

),

validation_errors as (

    select
        1
    from validation
        where count > 1

)

select *
from validation_errors

{% endtest %}