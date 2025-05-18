{{ config(
    materialized='table'
) }}

WITH humedad_silver AS (

    SELECT 
        fecha,
        cod_est,
        {{dbt_utils.generate_surrogate_key(['fecha','cod_est','Humedad_relativa_med'])}}::VARCHAR(250) AS humedad_id,
        ROUND(Humedad_relativa_max, 2) AS Humedad_relativa_max,
        ROUND(Humedad_relativa_min, 2) AS Humedad_relativa_min,
        ROUND(Humedad_relativa_med, 2) AS Humedad_relativa_med
      

    FROM {{ ref('hum_base') }}

)

SELECT * FROM humedad_silver