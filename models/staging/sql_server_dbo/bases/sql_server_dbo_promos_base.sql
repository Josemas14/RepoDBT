-- models/staging/stg_promos_base.sql
{{ config(materialized='ephemeral') }}

SELECT
    promo_id,
    discount,
    status,
    _fivetran_deleted,
    _fivetran_synced
FROM {{ source('sql_server_dbo', 'promos') }}

UNION ALL

SELECT
    'Sin descuento' AS promo_id,
    0 AS discount,
    'inactive' AS status,
    NULL AS _fivetran_deleted,
    NULL AS _fivetran_synced
