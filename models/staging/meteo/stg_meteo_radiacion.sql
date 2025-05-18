{{ config(
    materialized='table'
) }}

WITH Radiacion_silver AS (

    SELECT 
        fecha,
        cod_est,
        {{dbt_utils.generate_surrogate_key(['fecha','cod_est','RADIACION'])}}::VARCHAR(250) AS radiacion_id,
        ROUND(RADIACION, 2) AS Radiacion
      

    FROM {{ ref('rad_base') }}

)

SELECT * FROM Radiacion_silver