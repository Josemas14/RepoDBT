{{ config(
    materialized='table'
) }}
WITH date_spine AS (

    SELECT 
        DATEADD(DAY, SEQ4(), '2002-01-01') AS fecha
    FROM TABLE(GENERATOR(ROWCOUNT => 20000)) -- Ajusta según el rango de fechas que necesites

),

final AS (

    SELECT
        fecha,
        {{ dbt_utils.generate_surrogate_key(['fecha']) }}::VARCHAR(250) AS fecha_id,
        EXTRACT(YEAR FROM fecha) AS anio,
        EXTRACT(MONTH FROM fecha) AS mes,
        TO_CHAR(fecha, 'Month') AS nombre_mes,
        EXTRACT(DAY FROM fecha) AS dia_mes,
        EXTRACT(DAYOFWEEK FROM fecha) AS dia_semana,
         CASE EXTRACT(DAYOFWEEK FROM fecha)
            WHEN 0 THEN 'Domingo'
            WHEN 1 THEN 'Lunes'
            WHEN 2 THEN 'Martes'
            WHEN 3 THEN 'Miércoles'
            WHEN 4 THEN 'Jueves'
            WHEN 5 THEN 'Viernes'
            WHEN 6 THEN 'Sábado'
        END AS nombre_dia_semana,
        CASE WHEN EXTRACT(DAYOFWEEK FROM fecha) IN (0, 6) THEN 'Fin de Semana' ELSE 'Laboral' END AS tipo_dia,
        EXTRACT(QUARTER FROM fecha) AS trimestre,
        CASE WHEN EXTRACT(MONTH FROM fecha) IN (12,1,2) THEN 'Invierno'
             WHEN EXTRACT(MONTH FROM fecha) IN (3,4,5) THEN 'Primavera'
             WHEN EXTRACT(MONTH FROM fecha) IN (6,7,8) THEN 'Verano'
             ELSE 'Otoño' END AS "Estacion"
    FROM date_spine

)

SELECT * FROM final
