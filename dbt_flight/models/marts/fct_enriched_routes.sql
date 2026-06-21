{{ config(
    materialized='table',
    schema='AI'
) }}

select
    airline_code,
    airline_name,
    flight_number,
    route_key,
    origin_city,
    origin_country,
    destination_city,
    destination_country,
    distance_km,
    seats,
    aircraft_type,
    is_international,
    distance_category,

    snowflake.cortex.classify_text(
        concat(
            'Route from ', origin_city, ', ', origin_country,
            ' to ', destination_city, ', ', destination_country,
            '. Distance: ', distance_km, ' km.'
        ),
        ['DOMESTIC_SHORT', 'DOMESTIC_LONG', 'INTERNATIONAL_REGIONAL', 'INTERNATIONAL_LONG_HAUL']
    ):label::string as route_classification,

    snowflake.cortex.complete(
        'claude-4-sonnet',
        concat(
            'In one sentence, describe this flight route: ',
            airline_name, ' flight ', flight_number,
            ' from ', origin_city, ' (', origin_country, ')',
            ' to ', destination_city, ' (', destination_country, ')',
            ', distance ', distance_km, ' km, ',
            seats, ' seats on ', aircraft_type, '.'
        )
    ) as route_summary,

    current_timestamp() as enriched_at

from {{ ref('fct_clean_routes') }}
limit 20