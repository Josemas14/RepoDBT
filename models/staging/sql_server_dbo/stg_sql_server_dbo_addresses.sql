{{
  config(
    materialized='view'
    
  )
}}

WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

addresses_silver AS (
    SELECT 
        {{dbt_utils.generate_surrogate_key(['address_id'])}}::VARCHAR(250) AS budget_id
        , LPAD(zipcode,5,'0')::NUMBER(5,0) AS zipcode
        , country::VARCHAR(250)
        , address::VARCHAR(250)
        , state::VARCHAR(250)
    FROM src_addresses
)
SELECT * FROM addresses_silver