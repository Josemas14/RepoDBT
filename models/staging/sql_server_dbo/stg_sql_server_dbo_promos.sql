{{ config(materialized='view') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['promo_id']) }}::VARCHAR(250) AS promo_id,
    CAST(promo_id AS VARCHAR(250)) AS desc_promo,
    CAST(status AS VARCHAR(250)) AS status
FROM {{ ref('sql_server_dbo_promos_base') }}


