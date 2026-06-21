{{ config(
    materialized='table',
    schema='CURATED'
) }}

select * from {{ ref('stg_raw_routes') }}