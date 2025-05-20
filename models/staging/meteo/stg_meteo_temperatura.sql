{{ config(
    materialized='view'
) }}

WITH temperatura_silver AS (

    SELECT 
        fecha,
        cod_est,
        {{dbt_utils.generate_surrogate_key(['fecha','cod_est'])}}::VARCHAR(250) AS temperatura_id,
        COALESCE(ROUND(t_max, 2),0) AS temperatura_max,
        COALESCE(ROUND(t_min, 2),0) AS temperatura_min,
        COALESCE(ROUND(t_med, 2),0) AS temperatura_med

    FROM {{ ref('tem_base') }}
    where temperatura_max < 48 and temperatura_min > -40

),

duplicados AS (
    {{ dbt_utils.deduplicate(
        relation='temperatura_silver',
        partition_by='temperatura_id',
        order_by='temperatura_id'
    ) }}
)


SELECT * FROM duplicados
