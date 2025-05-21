{{ config(
    materialized='table',
    database='ALUMNO16_PRO_GOLD_DB',
    schema='analytics'
) }}

WITH hechos AS (
    SELECT * FROM {{ ref('fact_meteo') }}
),

joined_data AS (
    SELECT 
        h.fecha,
        h.estacion_id,
        t.temperatura_med,
        p.precipitacion,
        v.viento_max,
    FROM hechos h
    LEFT JOIN {{ ref('dim_temperatura') }} t ON h.temperatura_id = t.temperatura_id
    LEFT JOIN {{ ref('dim_precipitacion') }} p ON h.lluvia_id = p.lluvia_id
    LEFT JOIN {{ ref('dim_viento') }} v ON h.viento_id = v.viento_id

)

SELECT 
    fecha,
    estacion_id,
    CASE 
        WHEN temperatura_med < 10 THEN 'Abrigo'
        WHEN temperatura_med > 10 AND temperatura_med <= 12 THEN 'Chaqueta'
        WHEN temperatura_med > 13 AND temperatura_med <= 15 THEN 'Sudadera'
        WHEN temperatura_med > 16 AND temperatura_med <= 24 THEN 'Camiseta corta'
        WHEN temperatura_med > 25 THEN 'Bañador'
        ElSE 'Sudadera'
    END AS recomendacion_ropa,
    CASE 
        WHEN precipitacion > 0 THEN 'Llevar paraguas'
        ELSE 'Sin paraguas'
    END AS recomendacion_paraguas,
    CASE 
        WHEN temperatura_med > 25 AND precipitacion = 0 AND viento_max < 20 
             AND DAYOFWEEK(fecha) IN (0,6) THEN 'Pádel'
        ELSE 'Sin actividad recomendada'
    END AS actividad_recomendada
FROM joined_data
