{{ config(
    materialized='table'
) }}

WITH precipitacion_silver AS (

    SELECT 
        fecha,
        cod_est,
        {{dbt_utils.generate_surrogate_key(['fecha','cod_est','lluvia'])}}::VARCHAR(250) AS lluvia_id,
        ROUND(lluvia, 2) AS precipitacion,
      

    FROM {{ ref('pre_base') }}

)

SELECT * FROM precipitacion_silver
