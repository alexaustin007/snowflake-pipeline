  -- 03_raw_tables.sql                                                                                                                                                                     
  -- Creates the raw landing table that mirrors the source CSV structure.                                                                                                                  
  -- Raw layer = source of truth. No cleaning or transformation here. 

USE DATABASE FLIGHT_PIPELINE_DB;
USE SCHEMA RAW;

  CREATE OR REPLACE TABLE RAW_ROUTES (                                                                                                                                                     
    AIRLINE_CODE            STRING,
    AIRLINE_NAME            STRING,                                                                                                                                                        
    FLIGHT_NUMBER           STRING,
    ORIGIN_AIRPORT          STRING,                                                                                                                                                        
    ORIGIN_CITY             STRING,
    ORIGIN_COUNTRY          STRING,                                                                                                                                                        
    ORIGIN_REGION           STRING,                                                                                                                                                        
    ORIGIN_LATITUDE         FLOAT,
    ORIGIN_LONGITUDE        FLOAT,                                                                                                                                                         
    DESTINATION_AIRPORT     STRING,                                                                                                                                                        
    DESTINATION_CITY        STRING,
    DESTINATION_COUNTRY     STRING,                                                                                                                                                        
    DESTINATION_REGION      STRING,                                                                                                                                                        
    DESTINATION_LATITUDE    FLOAT,
    DESTINATION_LONGITUDE   FLOAT,                                                                                                                                                         
    DISTANCE_KM             FLOAT,                                                                                                                                                         
    SEATS                   NUMBER,
    AIRCRAFT_TYPE           STRING,                                                                                                                                                        
    CODESHARE               NUMBER,
    STOPS                   NUMBER,                                                                                                                                                        
    FLIGHT_DATE             DATE,
    FLIGHT_YEAR             NUMBER,                                                                                                                                                        
    FLIGHT_MONTH            NUMBER,                                                                                                                                                        
    FLIGHT_QUARTER          NUMBER,
    -- metadata columns                                                                                                                                                                    
    LOAD_TIMESTAMP          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE             STRING                                                                                                                                                         
  );                    