{{ config(
    materialized='view',
    schema='CURATED'
) }}

select
    upper(trim(airline_code))           as airline_code,
    trim(airline_name)                  as airline_name,
    upper(trim(flight_number))          as flight_number,
    upper(trim(origin_airport))         as origin_airport,
    trim(origin_city)                   as origin_city,
    trim(origin_country)                as origin_country,
    trim(origin_region)                 as origin_region,
    origin_latitude,
    origin_longitude,
    upper(trim(destination_airport))    as destination_airport,
    trim(destination_city)              as destination_city,
    trim(destination_country)           as destination_country,
    trim(destination_region)            as destination_region,
    destination_latitude,
    destination_longitude,
    distance_km,
    seats,
    upper(trim(aircraft_type))          as aircraft_type,
    codeshare,
    stops,
    flight_date,
    flight_year,
    flight_month,
    flight_quarter,
    upper(trim(origin_airport)) || '-' || upper(trim(destination_airport))  as route_key,
    case
        when trim(origin_country) <> trim(destination_country) then true
        else false
    end                                                                      as is_international,
    case
        when distance_km < 1500 then 'SHORT'
        when distance_km < 4000 then 'MEDIUM'
        else 'LONG'
    end                                                                      as distance_category,
    load_timestamp,
    source_file
from {{ source('raw', 'raw_routes') }}
where airline_code is not null
and flight_date is not null
and origin_airport is not null
and destination_airport is not null