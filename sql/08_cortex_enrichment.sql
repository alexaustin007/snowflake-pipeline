  -- Creates the AI/gold layer using Snowflake Cortex functions.                                                                                                                      
  -- AI_CLASSIFY basically CLASSIFY_TEXT adds a categorical label; AI_COMPLETE generates a natural-language route summary. 

USE DATABASE FLIGHT_PIPELINE_DB;                                                                                                                                                      
USE SCHEMA AI; 

  CREATE OR REPLACE TABLE ENRICHED_ROUTES AS                                                                                                                                            
  SELECT
    AIRLINE_CODE,                                                                                                                                                                       
    AIRLINE_NAME,                                                                                                                                                                     
    FLIGHT_NUMBER,                                                                                                                                                                      
    ROUTE_KEY,
    ORIGIN_CITY,                                                                                                                                                                        
    ORIGIN_COUNTRY,                                                                                                                                                                   
    DESTINATION_CITY,                                                                                                                                                                   
    DESTINATION_COUNTRY,
    DISTANCE_KM,                                                                                                                                                                        
    SEATS,                                                                                                                                                                            
    AIRCRAFT_TYPE,                                                                                                                                                                      
    IS_INTERNATIONAL,
    DISTANCE_CATEGORY,                                                                                                                                                                  
                                                                                                                                                                                        
    -- AI_CLASSIFY: assign a richer route category
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(                                                                                                                                                     
      CONCAT(                                                                                                                                                                           
        'Route from ', ORIGIN_CITY, ', ', ORIGIN_COUNTRY,
        ' to ', DESTINATION_CITY, ', ', DESTINATION_COUNTRY,                                                                                                                            
        '. Distance: ', DISTANCE_KM, ' km.'                                                                                                                                             
      ),                                                                                                                                                                                
      ['DOMESTIC_SHORT', 'DOMESTIC_LONG', 'INTERNATIONAL_REGIONAL', 'INTERNATIONAL_LONG_HAUL']                                                                                          
    ):label::STRING AS ROUTE_CLASSIFICATION,                                                                                                                                            
                                                                                                                                                                                        
    -- AI_COMPLETE: generate a human-readable route summary                                                                                                                             
    SNOWFLAKE.CORTEX.COMPLETE(                                                                                                                                                          
      'claude-4-sonnet',                                                                                                                                                                
      CONCAT(
        'In one sentence, describe this flight route: ',                                                                                                                                
        AIRLINE_NAME, ' flight ', FLIGHT_NUMBER,                                                                                                                                        
        ' from ', ORIGIN_CITY, ' (', ORIGIN_COUNTRY, ')',                                                                                                                               
        ' to ', DESTINATION_CITY, ' (', DESTINATION_COUNTRY, ')',                                                                                                                       
        ', distance ', DISTANCE_KM, ' km, ',                                                                                                                                            
        SEATS, ' seats on ', AIRCRAFT_TYPE, '.'                                                                                                                                         
      )                                                                                                                                                                                 
    ) AS ROUTE_SUMMARY,                                                                                                                                                                 
                                                                                                                                                                                        
    CURRENT_TIMESTAMP() AS ENRICHED_AT                                                                                                                                                
  FROM CURATED.CLEAN_ROUTES                                                                                                                                                             
  LIMIT 20;    
