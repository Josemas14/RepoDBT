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
    f.lluvia_id,
    f.estacion_id
  FROM {{ ref('fact_meteo') }} AS f
),

-- Asociamos cada hecho con la tabla de fechas para extraer el año
fechas AS (
  SELECT
    h.*,
    d.anio
  FROM hechos AS h
  JOIN {{ ref('dim_fecha') }} AS d
    ON h.fecha_id = d.fecha_id
),

-- Asociamos la precipitación
prec AS (
  SELECT
    f.*,
    p.precipitacion
  FROM fechas AS f
  JOIN {{ ref('dim_precipitacion') }} AS p
    ON f.lluvia_id = p.lluvia_id
),

-- Asociamos la provincia de cada estación
datos_finales AS (
  SELECT
    pr.anio,
    e.cod_provincia,
    CASE 
      WHEN e.cod_provincia = 'AL' THEN 'Almería'
      WHEN e.cod_provincia = 'CA' THEN 'Cádiz'
      WHEN e.cod_provincia = 'CO' THEN 'Córdoba'
      WHEN e.cod_provincia = 'GR' THEN 'Granada'
      WHEN e.cod_provincia = 'HU' THEN 'Huelva'
      WHEN e.cod_provincia = 'JA' THEN 'Jaén'
      WHEN e.cod_provincia = 'MA' THEN 'Málaga'
      WHEN e.cod_provincia = 'SE' THEN 'Sevilla'
      ELSE 'Desconocida'
    END AS provincia,
    pr.precipitacion
  FROM prec AS pr
  JOIN {{ ref('dim_estacion') }} AS e
    ON pr.estacion_id = e.estacion_id
)

SELECT
  anio,
  provincia,
  -- dividimos la suma anual por 12 para obtener la media mensual
  SUM(precipitacion) / 12.0/ 10 AS precipitacion_media_mensual
FROM datos_finales
GROUP BY 1, 2
ORDER BY 1, 2