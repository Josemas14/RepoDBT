{{ config(
    materialized='table'
) }}

WITH estacion_id_silver AS (

    SELECT 
        CAST(NOMBRE AS VARCHAR(250)) AS nombre_municipio,
        CAST(ID_EST AS VARCHAR(250)) AS cod_est,
        {{ dbt_utils.generate_surrogate_key(['ID_EST','MUNICIPIO']) }} AS estacion_id,
        CAST(ALTITUD AS FLOAT) AS altitud,
        CAST(LATITUD AS FLOAT) AS latitud,
        CAST(LONGITUD AS FLOAT) AS longitud,
        CAST(PROVINCIA AS INT) AS id_provincia,
        CAST(MUNICIPIO AS INT) AS id_municipio,
        LEFT(ID_EST, 2) AS cod_provincia,
        CASE 
            WHEN LEFT(ID_EST, 2) = 'AL' THEN 'Almería'
            WHEN LEFT(ID_EST, 2) = 'GR' THEN 'Granada'
            WHEN LEFT(ID_EST, 2) = 'SE' THEN 'Sevilla'
            WHEN LEFT(ID_EST, 2) = 'CA' THEN 'Cádiz'
            WHEN LEFT(ID_EST, 2) = 'MA' THEN 'Málaga'
            WHEN LEFT(ID_EST, 2) = 'HU' THEN 'Huelva'
            WHEN LEFT(ID_EST, 2) = 'CO' THEN 'Córdoba'
            WHEN LEFT(ID_EST, 2) = 'JA' THEN 'Jaén'
            ELSE 'Desconocida'
        END AS provincia

    FROM {{ source('meteo', 'identificacion_estacionesclima') }}

)

SELECT * FROM estacion_id_silver



