{% test date_not_before(model, column_name, min_date) %}

select * from {{ model }}
where {{ column_name }} < cast('{{ min_date }}' as date)

{% endtest %}
