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
        t.temperatura_max,
        t.temperatura_min,
        p.precipitacion,
        v.viento_max,
        r.radiacion
    FROM hechos h
    LEFT JOIN {{ ref('dim_temperatura') }} t ON h.temperatura_id = t.temperatura_id
    LEFT JOIN {{ ref('dim_precipitacion') }} p ON h.lluvia_id = p.lluvia_id
    LEFT JOIN {{ ref('dim_viento') }} v ON h.viento_id = v.viento_id
    LEFT JOIN {{ ref('dim_radiacion') }} r ON h.radiacion_id = r.radiacion_id
)

SELECT 
    fecha,
    estacion_id,
    CASE WHEN temperatura_max > 38 THEN 'Calor_extremo' END AS fenomeno_ola_calor,
    CASE WHEN temperatura_min < 0 THEN 'Heladas' END AS fenomeno_helada,
    CASE WHEN precipitacion > 50 THEN 'Lluvia_torrencial' END AS fenomeno_lluvia_extrema,
    CASE WHEN viento_max > 80 THEN 'Viento_fuerte' END AS fenomeno_racha_viento,
    CASE WHEN radiacion > 25 THEN 'Radiacion_alta' END AS fenomeno_radiacion_alta
FROM joined_data
