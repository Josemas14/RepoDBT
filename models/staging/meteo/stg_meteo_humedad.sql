{{ config(
    materialized='table'
) }}


 WITH humedad_silver AS (
    SELECT 
        fecha,
        cod_est,
        {{ dbt_utils.generate_surrogate_key(['fecha','cod_est','Humedad_relativa_med']) }}::VARCHAR(250) AS humedad_id,
        COALESCE(ROUND(Humedad_relativa_max, 2),0) AS Humedad_relativa_max,
        COALESCE(ROUND(Humedad_relativa_min, 2),0) AS Humedad_relativa_min,
        COALESCE(ROUND(Humedad_relativa_med, 2),0) AS Humedad_relativa_med
    FROM {{ref('hum_base')}}
),
duplicados AS (
    {{ dbt_utils.deduplicate(
        relation='humedad_silver',
        partition_by='humedad_id',
        order_by='humedad_id'
    ) }}
)

SELECT * FROM duplicados
