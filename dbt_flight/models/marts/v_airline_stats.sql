{{ config(
    materialized='view',
    schema='CURATED'
) }}

select
    airline_code,
    airline_name,
    count(*)                                                        as total_flights,
    count(distinct route_key)                                       as unique_routes,
    count(distinct origin_country)                                  as countries_served,
    round(avg(distance_km), 1)                                      as avg_distance_km,
    sum(iff(is_international, 1, 0))                                as international_flights,
    round(100.0 * sum(iff(is_international, 1, 0)) / count(*), 1)  as pct_international
from {{ ref('fct_clean_routes') }}
group by airline_code, airline_name
order by total_flights desc