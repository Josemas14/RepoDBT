{{ config(
    materialized='table'
) }}


SELECT
    temperatura_id,
    temperatura_max,
    temperatura_min,
    temperatura_med,
    CASE
        WHEN temperatura_med < 10 THEN 'Fría'
        WHEN temperatura_med BETWEEN 10 AND 20 THEN 'Suave'
        ELSE 'Cálida'
    END AS categoria_temperatura
FROM {{ ref('stg_meteo_temperatura') }}

