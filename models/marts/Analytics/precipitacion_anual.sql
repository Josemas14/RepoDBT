{{ config(
    materialized='table',
    database='ALUMNO16_PRO_GOLD_DB',
    schema='analytics'
) }}

SELECT
  d.anio           AS anio,
  SUM(p.precipitacion)  AS precipitacion_anual
FROM fact_meteo      f
-- Unimos con la dimensión de fecha para extraer Año
JOIN dim_fecha       d ON f.fecha_id = d.fecha_id
-- Unimos con la dimensión de precipitación para obtener el valor
JOIN dim_precipitacion p ON f.lluvia_id = p.lluvia_id
GROUP BY 1
ORDER BY 1;
