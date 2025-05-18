{{ config(
    materialized='table',
    database='ALUMNO16_DEV_GOLD_DB',
    schema='meteo'
) }}

SELECT 
    radiacion_id,
    radiacion,
    CASE
        WHEN radiacion < 100 THEN 'Baja'
        WHEN radiacion BETWEEN 100 AND 500 THEN 'Media'
        ELSE 'Alta'
    END AS nivel_radiacion
FROM {{ ref('stg_meteo_radiacion') }}
