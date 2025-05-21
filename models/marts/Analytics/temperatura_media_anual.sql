{{  
  config(
    materialized='table',
    database='ALUMNO16_PRO_GOLD_DB',
    schema='analytics'
  ) 
}}

WITH hechos AS (
  SELECT
    f.fecha_id,
    f.temperatura_id,
    e.cod_provincia
  FROM {{ ref('fact_meteo') }} AS f
  JOIN {{ ref('dim_estacion') }} AS e
    ON f.estacion_id = e.estacion_id
),

fechas AS (
  SELECT
    h.*,
    d.anio
  FROM hechos AS h
  JOIN {{ ref('dim_fecha') }} AS d
    ON h.fecha_id = d.fecha_id
),

temps AS (
  SELECT
    f.anio,
    f.cod_provincia,
    t.temperatura_med
  FROM fechas AS f
  JOIN {{ ref('dim_temperatura') }} AS t
    ON f.temperatura_id = t.temperatura_id
)

SELECT
  anio,
  cod_provincia,
  ROUND(AVG(temperatura_med), 2) AS temperatura_media_anual
FROM temps
GROUP BY 1, 2
ORDER BY 1, 2
