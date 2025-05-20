{{ config(
    materialized='table'
) }}

SELECT
    humedad_id,
    humedad_relativa_max,
    humedad_relativa_min,
    humedad_relativa_med,
    CASE
        WHEN humedad_relativa_med < 30 THEN 'Baja'
        WHEN humedad_relativa_med BETWEEN 30 AND 60 THEN 'Media'
        ELSE 'Alta'
    END AS humedad_categoria
FROM {{ ref('stg_meteo_humedad') }}
