
{{
  config(
    materialized='view'
    
  )
}}

WITH src_budget AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
    ),

budget_silver AS (
    SELECT 
        {{dbt_utils.generate_surrogate_key(['product_id','month'])}} AS budget_id
        , product_id
        , quantity AS quantity_product
        , month
    FROM src_budget
)
SELECT * FROM budget_silver