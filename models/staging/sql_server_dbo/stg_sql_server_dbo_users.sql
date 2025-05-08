
{{
  config(
    materialized='view'
    
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

promos_silver AS (
    SELECT 
        {{dbt_utils.generate_surrogate_key(['promo_id'])}} AS promo_id
        , promo_id as desc_promo
        , status
        
    FROM src_promos
)
SELECT * FROM promos_silver