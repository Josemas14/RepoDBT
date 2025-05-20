{{ config(
    materialized='view'
) }}


WITH precipitacion_silver AS (

    SELECT 
        fecha,
        cod_est,
        {{dbt_utils.generate_surrogate_key(['fecha','cod_est','lluvia'])}}::VARCHAR(250) AS lluvia_id,
        COALESCE(ROUND(lluvia, 2),0) AS precipitacion,
      

    FROM {{ ref('pre_base') }}

),
duplicados AS (
    {{ dbt_utils.deduplicate(
        relation='precipitacion_silver',
        partition_by='lluvia_id',
        order_by='lluvia_id'
    ) }}
)

SELECT * FROM duplicados
 