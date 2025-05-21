{{ config(
    materialized='view',
    database='ALUMNO16_PRO_SILVER_DB',
    schema='meteo'
) }}


WITH Radiacion_silver AS (

    SELECT 
        fecha,
        cod_est,
        {{dbt_utils.generate_surrogate_key(['fecha','cod_est','RADIACION'])}}::VARCHAR(250) AS radiacion_id,
        COALESCE(ROUND(RADIACION, 2),0) AS Radiacion
      

    FROM {{ ref('rad_base') }}

),
duplicados AS (
    {{ dbt_utils.deduplicate(
        relation='Radiacion_silver',
        partition_by='radiacion_id',
        order_by='radiacion_id'
    ) }}
)


SELECT * FROM duplicados