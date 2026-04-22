  -- Creates a task that incrementally processes new rows from the stream                                                                                                                          
  -- and inserts cleaned/transformed data into the curated layer.

  USE DATABASE FLIGHT_PIPELINE_DB;                                                                                                                                                                 
  USE SCHEMA CURATED; 

    CREATE OR REPLACE TASK PROCESS_NEW_ROUTES                                                                                                                                                        
    WAREHOUSE = PIPELINE_WH
    SCHEDULE = '5 minute'                                                                                                                                                                          
    WHEN SYSTEM$STREAM_HAS_DATA('FLIGHT_PIPELINE_DB.RAW.RAW_ROUTES_STREAM')                                                                                                                        
  AS                                                                                                                                                                                               
  INSERT INTO CURATED.CLEAN_ROUTES (                                                                                                                                                               
    AIRLINE_CODE, AIRLINE_NAME, FLIGHT_NUMBER,                                                                                                                                                     
    ORIGIN_AIRPORT, ORIGIN_CITY, ORIGIN_COUNTRY, ORIGIN_REGION,
    ORIGIN_LATITUDE, ORIGIN_LONGITUDE,                                                                                                                                                             
    DESTINATION_AIRPORT, DESTINATION_CITY, DESTINATION_COUNTRY, DESTINATION_REGION,                                                                                                                
    DESTINATION_LATITUDE, DESTINATION_LONGITUDE,                                                                                                                                                   
    DISTANCE_KM, SEATS, AIRCRAFT_TYPE, CODESHARE, STOPS,                                                                                                                                           
    FLIGHT_DATE, FLIGHT_YEAR, FLIGHT_MONTH, FLIGHT_QUARTER,                                                                                                                                        
    ROUTE_KEY, IS_INTERNATIONAL, DISTANCE_CATEGORY,                                                                                                                                                
    SOURCE_FILE                                                                                                                                                                                    
  )                                                                                                                                                                                                
  SELECT          
    UPPER(TRIM(AIRLINE_CODE)),                                                                                                                                                                     
    TRIM(AIRLINE_NAME),
    UPPER(TRIM(FLIGHT_NUMBER)),                                                                                                                                                                    
    UPPER(TRIM(ORIGIN_AIRPORT)),
    TRIM(ORIGIN_CITY),                                                                                                                                                                             
    TRIM(ORIGIN_COUNTRY),
    TRIM(ORIGIN_REGION),                                                                                                                                                                           
    ORIGIN_LATITUDE,
    ORIGIN_LONGITUDE,                                                                                                                                                                              
    UPPER(TRIM(DESTINATION_AIRPORT)),
    TRIM(DESTINATION_CITY),                                                                                                                                                                        
    TRIM(DESTINATION_COUNTRY),
    TRIM(DESTINATION_REGION),                                                                                                                                                                      
    DESTINATION_LATITUDE,
    DESTINATION_LONGITUDE,                                                                                                                                                                         
    DISTANCE_KM,
    SEATS,                                                                                                                                                                                         
    UPPER(TRIM(AIRCRAFT_TYPE)),
    CODESHARE,                                                                                                                                                                                     
    STOPS,
    FLIGHT_DATE,                                                                                                                                                                                   
    FLIGHT_YEAR,  
    FLIGHT_MONTH,                                                                                                                                                                                  
    FLIGHT_QUARTER,
    -- ROUTE_KEY: origin-destination airport pair                                                                                                                                                  
    UPPER(TRIM(ORIGIN_AIRPORT)) || '-' || UPPER(TRIM(DESTINATION_AIRPORT)),                                                                                                                        
    -- IS_INTERNATIONAL: true when countries differ                                                                                                                                                
    CASE                                                                                                                                                                                           
      WHEN TRIM(ORIGIN_COUNTRY) <> TRIM(DESTINATION_COUNTRY) THEN TRUE                                                                                                                             
      ELSE FALSE                                                                                                                                                                                   
    END,          
    -- DISTANCE_CATEGORY: short / medium / long                                                                                                                                                    
    CASE          
      WHEN DISTANCE_KM < 1500 THEN 'SHORT'
      WHEN DISTANCE_KM < 4000 THEN 'MEDIUM'                                                                                                                                                        
      ELSE 'LONG'
    END,                                                                                                                                                                                           
    SOURCE_FILE   
  FROM RAW.RAW_ROUTES_STREAM                                                                                                                                                                       
  WHERE METADATA$ACTION = 'INSERT'
    AND AIRLINE_CODE IS NOT NULL                                                                                                                                                                   
    AND FLIGHT_DATE IS NOT NULL
    AND ORIGIN_AIRPORT IS NOT NULL                                                                                                                                                                 
    AND DESTINATION_AIRPORT IS NOT NULL;
                                                                                                                                                                                                   
  -- Tasks are in SUSPENDED state - must be resumed
  ALTER TASK PROCESS_NEW_ROUTES RESUME;                                                                                                                                                            
                                                                                                                                                                                                   
  SHOW TASKS IN SCHEMA CURATED;       
