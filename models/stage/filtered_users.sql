-- models/filtered_users.sql

{{ config(materialized='table') }}

{% set is_active_filter = var('is_active_filter', None) %}
{% set start_date = var('start_date', None) %}
{% set end_date = var('end_date', None) %}

WITH base AS (
    SELECT
        id,
        user_name,
        email,
        sign_up_date,
        last_login,
        is_active,
        score
    FROM {{ source('my_database', 'test_model') }}
    WHERE
        ({% if is_active_filter != None %} is_active = {{ is_active_filter }}{% endif %})
        {% if is_active_filter != None and (start_date != None or end_date != None) %} AND {% endif %}
        ({% if start_date != None %} last_login >= '{{ start_date }}' {% endif %})
        {% if start_date != None and end_date != None %} AND {% endif %}
        ({% if end_date != None %} last_login <= '{{ end_date }}' {% endif %})
)

SELECT
    id,
    user_name,
    email,
    sign_up_date,
    last_login,
    is_active,
    score
FROM base
