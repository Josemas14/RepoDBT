{{ config(
    materialized='incremental',
    database='ALUMNO16_PRO_GOLD_DB',
    schema='meteo'
) }}
SELECT
    estacion_id,
    nombre_municipio,
    cod_est,
    altitud,
    latitud,
    longitud,
    id_provincia,
    id_municipio,
    cod_provincia,
    provincia
FROM {{ ref('stg_meteo_ide_estacion') }}