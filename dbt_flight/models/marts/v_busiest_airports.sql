{{ config(
    materialized='view',
    schema='CURATED'
) }}

select
    airport_code,
    city,
    country,
    count(*)                    as total_appearances,
    count(distinct route_key)   as unique_routes
from (
    select origin_airport as airport_code, origin_city as city, origin_country as country, route_key
    from {{ ref('fct_clean_routes') }}
    union all
    select destination_airport, destination_city, destination_country, route_key
    from {{ ref('fct_clean_routes') }}
)
group by airport_code, city, country
order by total_appearances desc