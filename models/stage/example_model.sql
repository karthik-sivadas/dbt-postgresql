-- models/example_model.sql

{{ config(materialized='table') }}

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
)

SELECT
    COUNT(*) AS total_users,
    AVG(score) AS average_score,
    ROUND(100.0 * SUM(CASE WHEN is_active THEN 1 ELSE 0 END) / COUNT(*), 2) AS percentage_active_users
FROM base
