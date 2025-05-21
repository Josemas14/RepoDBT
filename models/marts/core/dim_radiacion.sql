{{ config(
    materialized='table',
    database='ALUMNO16_PRO_GOLD_DB',
    schema='meteo'
) }}

SELECT 
    radiacion_id,
    radiacion,
    CASE
        WHEN radiacion < 15 THEN 'Baja'
        WHEN radiacion BETWEEN 15 AND 25 THEN 'Media'
        ELSE 'Alta'
    END AS nivel_radiacion
FROM {{ ref('stg_meteo_radiacion') }}
