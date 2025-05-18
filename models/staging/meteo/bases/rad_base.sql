{{ config(
    materialized='view'
) }}

with base_2002 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2002') }}

), base_2003 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2003') }}

),  base_2004 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2004') }}
    ),
    base_2005 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2005') }}
    ),
    base_2006 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2006') }}
    ),  
    base_2007 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2007') }}
    ),
    base_2008 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2008') }}
    ),
    base_2009 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2009') }}
    ),
    base_2010 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2010') }}
    ),
    base_2011 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo','clim_diario_2011') }}
    ),
    base_2012 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2012') }}
    ),
    base_2013 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2013') }}
    ),
    base_2014 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2014') }}
    ),
    base_2015 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2015') }}
    ),
    base_2016 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2016') }}
    ),
base_2017 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2017') }}
    ),
base_2018 as (

    select 
        fecha::date as fecha,
        cod_est,
        PROMEDIODERADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2018') }}
    ),
    base_2019 as (

    select 
        fecha::date as fecha,
        cod_est,
        PROMEDIODERADIACION:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2019') }}
    ),
    base_2020 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADICION_MED:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2020') }}
    ),
    base_2021 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADICION_MED:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2021') }}
    ),
    base_2022 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADICION_MED:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2022') }}
    ),
    base_2023 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADICION_MED:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2023') }}
    ),
    base_2024 as (

    select 
        fecha::date as fecha,
        cod_est,
        RADICION_MED:: FLOAT as RADIACION
    from {{ source('meteo', 'clim_diario_2024') }}
    ),
unioned_data as (

    select * from base_2002
    union all
    select * from base_2003
    union all 
    select * from base_2004
    union all
    select * from base_2005
    union all 
    select * from base_2006
    union all
    select * from base_2007
    union all 
    select * from base_2008
    union all
    select * from base_2009
    union all 
    select * from base_2010
    union all
    select * from base_2011
    union all 
    select * from base_2012
    union all
    select * from base_2013
    union all 
    select * from base_2014
     union all 
    select * from base_2015
    union all
    select * from base_2016
    union all 
    select * from base_2017
    union all
    select * from base_2018
    union all 
    select * from base_2019
     union all 
    select * from base_2020
    union all
    select * from base_2021
    union all 
    select * from base_2022
    union all
    select * from base_2023
    union all 
    select * from base_2024

)

select * from unioned_data
