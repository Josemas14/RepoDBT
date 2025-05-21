{{ config(
    materialized='table',
    database='ALUMNO16_PRO_GOLD_DB',
    schema='analytics'
) }}

SELECT
  d.anio                      AS anio,
  sum(p.precipitacion)        AS precipitacion_anual
FROM {{ ref('fact_meteo') }}              f
JOIN {{ ref('dim_fecha') }}       d ON f.fecha_id = d.fecha_id
JOIN {{ ref('dim_precipitacion') }} p ON f.lluvia_id = p.lluvia_id
GROUP BY 1
ORDER BY 1
