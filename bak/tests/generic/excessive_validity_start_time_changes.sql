{% test excessive_validity_start_time_changes(model, column_name, PKs, threshold) %}

with validation as (

    select
        count(distinct validity_start_time) as validity_start_time_changes

    from {{ model }}

    group by {{ PKs }}

),

validation_errors as (

    select
        1

    from validation
        where validity_start_time_changes > {{ threshold }}

)

select *
from validation_errors

{% endtest %}