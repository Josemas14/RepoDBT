{{ config(
    materialized='table',
    database='ALUMNO16_DEV_GOLD_DB',
    schema='meteo'
) }}

WITH precip_categoria AS (
    SELECT
        lluvia_id,
        precipitacion,
        CASE 
            WHEN precipitacion = 0 THEN 'Sin lluvia'
            WHEN precipitacion > 0 AND precipitacion <= 2.5 THEN 'Lluvia ligera'
            WHEN precipitacion > 2.5 AND precipitacion <= 7.6 THEN 'Lluvia moderada'
            WHEN precipitacion > 7.6 THEN 'Lluvia fuerte'
            ELSE 'Desconocida'
        END AS categoria_lluvia
    FROM {{ ref('stg_meteo_precipitacion') }}
)

SELECT 
    lluvia_id,
    categoria_lluvia,
    precipitacion
FROM precip_categoria
