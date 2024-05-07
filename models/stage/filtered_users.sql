-- models/filtered_users.sql

{{ config(materialized='table') }}

{% set is_active_filter = var('is_active_filter', None) %}
{% set start_date = var('start_date', None) %}
{% set end_date = var('end_date', None) %}
{% set conditions = [] %}

{% if is_active_filter != None %}
  {% do conditions.append("is_active = " + is_active_filter | string) %}
{% endif %}

{% if start_date != None %}
  {% do conditions.append("last_login >= '" + start_date + "'") %}
{% endif %}

{% if end_date != None %}
  {% do conditions.append("last_login <= '" + end_date + "'") %}
{% endif %}

WITH base AS (
    SELECT
        id,
        user_name,
        email,
        sign_up_date,
        last_login,
        is_active,
        score
    FROM {{ source('postgres', 'test_model') }}
    {% if conditions %}
    WHERE {{ conditions | join(' AND ') }}
    {% endif %}
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
