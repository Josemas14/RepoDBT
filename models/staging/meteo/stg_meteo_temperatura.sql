{{ config(
    materialized='table'
) }}

WITH temperatura_silver AS (

    SELECT 
        fecha,
        cod_est,
        {{dbt_utils.generate_surrogate_key(['fecha','cod_est'])}}::VARCHAR(250) AS temperatura_id,
        ROUND(t_max, 2) AS temperatura_max,
        ROUND(t_min, 2) AS temperatura_min,
        ROUND(t_med, 2) AS temperatura_med

    FROM {{ ref('tem_base') }}

)

SELECT * FROM temperatura_silver
