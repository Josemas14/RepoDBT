{{ config(
    materialized='table'
) }}

WITH viento_silver AS (

    SELECT 
        fecha,
        cod_est,
        {{dbt_utils.generate_surrogate_key(['fecha','cod_est','v_med'])}}::VARCHAR(250) AS viento_id,
        ROUND(v_max, 2) AS viento_max,
        ROUND(v_min, 2) AS viento_min,
        ROUND(v_med, 2) AS viento_med,
        ROUND(cv1, 2) AS minutos_direccion_1,
        ROUND(cv2, 2) AS minutos_direccion_2,
        ROUND(cv3, 2) AS minutos_direccion_3,
        ROUND(cv4, 2) AS minutos_direccion_4,
        ROUND(direccion, 2) AS direccion,

    FROM {{ ref('viento_base') }}

)

SELECT * FROM viento_silver