{{ config(
    materialized='table',
    database='ALUMNO16_PRO_GOLD_DB',
    schema='analytics'
) }}

WITH temp_granada AS (
    SELECT 
        f.fecha,
        t.temperatura_max
    FROM {{ ref('fact_meteo') }} f
    LEFT JOIN {{ ref('dim_temperatura') }} t
        ON f.temperatura_id = t.temperatura_id
    LEFT JOIN {{ ref('dim_estacion') }} e
        ON f.estacion_id = e.estacion_id
    WHERE e.provincia = 'Granada'
),

max_temp AS (
    SELECT MAX(temperatura_max) AS temperatura_max_granada
    FROM temp_granada
)

SELECT 
    g.fecha, 
    g.temperatura_max
FROM temp_granada g
JOIN max_temp m
    ON g.temperatura_max = m.temperatura_max_granada
