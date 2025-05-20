{{ config(
    materialized='view'
) }}

WITH viento_silver AS (

    SELECT 
        fecha,
        cod_est,
        {{dbt_utils.generate_surrogate_key(['fecha','cod_est','v_med'])}}::VARCHAR(250) AS viento_id,
        COALESCE(ROUND(v_max, 2), 0) AS viento_max,
        COALESCE(ROUND(v_min, 2), 0) AS viento_min,
        COALESCE(ROUND(v_med, 2), 0) AS viento_med,
        COALESCE(ROUND(cv1, 2), 0) AS minutos_direccion_1,
        COALESCE(ROUND(cv2, 2), 0) AS minutos_direccion_2,
        COALESCE(ROUND(cv3, 2), 0) AS minutos_direccion_3,
        COALESCE(ROUND(cv4, 2), 0) AS minutos_direccion_4,
        COALESCE(ROUND(direccion, 2), 0) AS direccion

    FROM {{ ref('viento_base') }}

),
duplicados AS (
    {{ dbt_utils.deduplicate(
        relation='viento_silver',
        partition_by='viento_id',
        order_by='viento_id'
    ) }}
)


SELECT * FROM duplicados