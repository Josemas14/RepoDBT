{{ config(
    materialized='table',
    database='ALUMNO16_DEV_GOLD_DB',
    schema='meteo'
) }}



WITH joined_data AS (

    SELECT 
        t.fecha,
        e.estacion_id,
        h.humedad_id,
        p.lluvia_id,
        r.radiacion_id,
        t.temperatura_id,
        v.viento_id
    FROM {{ ref('stg_meteo_temperatura') }} t
    LEFT JOIN {{ ref('stg_meteo_precipitacion') }} p
        ON t.fecha = p.fecha AND t.cod_est = p.cod_est
    LEFT JOIN {{ ref('stg_meteo_humedad') }} h
        ON t.fecha = h.fecha AND t.cod_est = h.cod_est
    LEFT JOIN {{ ref('stg_meteo_viento') }} v
        ON t.fecha = v.fecha AND t.cod_est = v.cod_est
    LEFT JOIN {{ ref('stg_meteo_radiacion') }} r
        ON t.fecha = r.fecha AND t.cod_est = r.cod_est
    LEFT JOIN {{ ref('stg_meteo_ide_estacion') }} e
        ON t.cod_est = e.cod_est

)

SELECT * FROM joined_data