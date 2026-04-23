  -- Creates the consumption layer: reporting views over curated and AI data.                                                                                                                    
  -- These are the views that dashboards, BI tools, and apps query directly.

USE DATABASE FLIGHT_PIPELINE_DB;                                                                                                                                                                 
USE SCHEMA CURATED;       

  -- View 1: Per-route summary                                                                                                                                                                   
  -- One row per unique origin-destination route with key facts
  CREATE OR REPLACE VIEW V_ROUTE_SUMMARY AS                                                                                                                                                        
  SELECT                                                                                                                                                                                           
    ROUTE_KEY,                                                                                                                                                                                     
    ORIGIN_AIRPORT,                                                                                                                                                                                
    ORIGIN_CITY,                                                                                                                                                                                 
    ORIGIN_COUNTRY,
    DESTINATION_AIRPORT,                                                                                                                                                                           
    DESTINATION_CITY,
    DESTINATION_COUNTRY,                                                                                                                                                                           
    IS_INTERNATIONAL,                                                                                                                                                                            
    DISTANCE_CATEGORY,                                                                                                                                                                             
    ROUND(AVG(DISTANCE_KM), 1) AS AVG_DISTANCE_KM,
    COUNT(*) AS FLIGHT_COUNT,                                                                                                                                                                      
    COUNT(DISTINCT AIRLINE_CODE) AS AIRLINE_COUNT,                                                                                                                                               
    SUM(SEATS) AS TOTAL_SEATS,                                                                                                                                                                     
    MIN(FLIGHT_DATE) AS FIRST_FLIGHT,                                                                                                                                                              
    MAX(FLIGHT_DATE) AS LAST_FLIGHT                                                                                                                                                                
  FROM CLEAN_ROUTES                                                                                                                                                                                
  GROUP BY                                                                                                                                                                                       
    ROUTE_KEY, ORIGIN_AIRPORT, ORIGIN_CITY, ORIGIN_COUNTRY,                                                                                                                                        
    DESTINATION_AIRPORT, DESTINATION_CITY, DESTINATION_COUNTRY,                                                                                                                                    
    IS_INTERNATIONAL, DISTANCE_CATEGORY;                                                                                                                                                           
                                                                                                                                                                                                   
  -- View 2: Per-airline statistics                                                                                                                                                                
  -- Aggregates by airline — great for comparing carriers                                                                                                                                        
  CREATE OR REPLACE VIEW V_AIRLINE_STATS AS                                                                                                                                                        
  SELECT                                                                                                                                                                                           
    AIRLINE_CODE,
    AIRLINE_NAME,                                                                                                                                                                                  
    COUNT(*) AS TOTAL_FLIGHTS,                                                                                                                                                                   
    COUNT(DISTINCT ROUTE_KEY) AS UNIQUE_ROUTES,                                                                                                                                                    
    COUNT(DISTINCT ORIGIN_COUNTRY) AS COUNTRIES_SERVED,
    ROUND(AVG(DISTANCE_KM), 1) AS AVG_DISTANCE_KM,                                                                                                                                                 
SUM(IFF(IS_INTERNATIONAL, 1, 0)) AS INTERNATIONAL_FLIGHTS,                                                                                                                                       
  ROUND(100.0 * SUM(IFF(IS_INTERNATIONAL, 1, 0)) / COUNT(*), 1) AS PCT_INTERNATIONAL                                                                                                             
  FROM CLEAN_ROUTES                                                                                                                                                                                
  GROUP BY AIRLINE_CODE, AIRLINE_NAME                                                                                                                                                              
  ORDER BY TOTAL_FLIGHTS DESC;                                                                                                                                                                     
                                                                                                                                                                                                   
  -- View 3: Busiest airports (as origin OR destination)                                                                                                                                           
  -- Uses UNION ALL to count airport appearances in either role
  CREATE OR REPLACE VIEW V_BUSIEST_AIRPORTS AS                                                                                                                                                     
  SELECT                                                                                                                                                                                           
    AIRPORT_CODE,                                                                                                                                                                                  
    CITY,                                                                                                                                                                                          
    COUNTRY,                                                                                                                                                                                     
    COUNT(*) AS TOTAL_APPEARANCES,                                                                                                                                                                 
    COUNT(DISTINCT ROUTE_KEY) AS UNIQUE_ROUTES
  FROM (                                                                                                                                                                                           
    SELECT ORIGIN_AIRPORT AS AIRPORT_CODE, ORIGIN_CITY AS CITY, ORIGIN_COUNTRY AS COUNTRY, ROUTE_KEY                                                                                             
    FROM CLEAN_ROUTES                                                                                                                                                                              
    UNION ALL                                                                                                                                                                                    
    SELECT DESTINATION_AIRPORT, DESTINATION_CITY, DESTINATION_COUNTRY, ROUTE_KEY                                                                                                                   
    FROM CLEAN_ROUTES                                                                                                                                                                              
  )
  GROUP BY AIRPORT_CODE, CITY, COUNTRY                                                                                                                                                             
  ORDER BY TOTAL_APPEARANCES DESC;                                                                                                                                                                 
   
  -- View 4: Enriched dashboard view                                                                                                                                                               
  -- Joins curated data with Cortex AI enrichment — final shape for apps/dashboards                                                                                                              
  CREATE OR REPLACE VIEW AI.V_ENRICHED_ROUTES_DASHBOARD AS                                                                                                                                         
  SELECT                                                                                                                                                                                           
    c.AIRLINE_CODE,                                                                                                                                                                                
    c.AIRLINE_NAME,                                                                                                                                                                                
    c.FLIGHT_NUMBER,                                                                                                                                                                             
    c.ROUTE_KEY,                                                                                                                                                                                   
    c.ORIGIN_CITY,
    c.ORIGIN_COUNTRY,                                                                                                                                                                              
    c.DESTINATION_CITY,                                                                                                                                                                          
    c.DESTINATION_COUNTRY,                                                                                                                                                                         
    c.DISTANCE_KM,
    c.DISTANCE_CATEGORY,                                                                                                                                                                           
    c.IS_INTERNATIONAL,                                                                                                                                                                          
    c.SEATS,                                                                                                                                                                                       
    c.AIRCRAFT_TYPE,
    e.ROUTE_CLASSIFICATION,                                                                                                                                                                        
    e.ROUTE_SUMMARY,                                                                                                                                                                               
    e.ENRICHED_AT
  FROM CURATED.CLEAN_ROUTES c                                                                                                                                                                      
  LEFT JOIN AI.ENRICHED_ROUTES e                                                                                                                                                                   
    ON c.ROUTE_KEY = e.ROUTE_KEY
   AND c.FLIGHT_NUMBER = e.FLIGHT_NUMBER;                                                                                                                                                          