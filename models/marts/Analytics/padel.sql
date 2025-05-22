{{ config(
    materialized='table',
    database='ALUMNO16_PRO_GOLD_DB',
    schema='analytics'
) }}

SELECT 
    d.anio,
    e.nombre_municipio,
    COUNT(*) AS total_dias_padel
FROM {{ ref('recomendacion') }} r
JOIN {{ ref('dim_fecha') }} d
    ON r.fecha = d.fecha
JOIN {{ ref('dim_estacion') }} e
    ON r.estacion_id = e.estacion_id
WHERE r.actividad_recomendada = 'Pádel' and anio= 2024 and nombre_municipio = 'Lecrín'
GROUP BY d.anio,nombre_municipio
ORDER BY d.anio,nombre_municipio

