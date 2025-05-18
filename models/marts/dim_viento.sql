{{ config(
    materialized='table',
    database='ALUMNO16_DEV_GOLD_DB',
    schema='meteo'
) }}

WITH viento AS (
    SELECT 
        viento_id,
        viento_max,
        viento_min,
        viento_med,
        minutos_direccion_1,
        minutos_direccion_2,
        minutos_direccion_3,
        minutos_direccion_4,
        direccion,
        CASE
            WHEN viento_max < 5 THEN 'Calma'
            WHEN viento_max >= 5 AND viento_max < 20 THEN 'Viento suave'
            WHEN viento_max >= 20 AND viento_max < 40 THEN 'Viento moderado'
            WHEN viento_max >= 40 AND viento_max < 60 THEN 'Viento fuerte'
            ELSE 'Viento muy fuerte'
        END AS categoria_viento
    FROM {{ ref('stg_meteo_viento') }}
)

SELECT * FROM viento
