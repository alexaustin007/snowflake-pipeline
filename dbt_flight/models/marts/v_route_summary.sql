{{ config(
    materialized='view',
    schema='CURATED'
) }}

select
    route_key,
    origin_airport,
    origin_city,
    origin_country,
    destination_airport,
    destination_city,
    destination_country,
    is_international,
    distance_category,
    round(avg(distance_km), 1)      as avg_distance_km,
    count(*)                         as flight_count,
    count(distinct airline_code)     as airline_count,
    sum(seats)                       as total_seats,
    min(flight_date)                 as first_flight,
    max(flight_date)                 as last_flight
from {{ ref('fct_clean_routes') }}
group by
    route_key, origin_airport, origin_city, origin_country,
    destination_airport, destination_city, destination_country,
    is_international, distance_category
