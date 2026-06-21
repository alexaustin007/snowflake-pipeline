{{ config(
    materialized='view',
    schema='AI'
) }}

select
    c.airline_code,
    c.airline_name,
    c.flight_number,
    c.route_key,
    c.origin_city,
    c.origin_country,
    c.destination_city,
    c.destination_country,
    c.distance_km,
    c.distance_category,
    c.is_international,
    c.seats,
    c.aircraft_type,
    e.route_classification,
    e.route_summary,
    e.enriched_at
from {{ ref('fct_clean_routes') }} c
left join {{ ref('fct_enriched_routes') }} e
    on c.route_key = e.route_key
    and c.flight_number = e.flight_number