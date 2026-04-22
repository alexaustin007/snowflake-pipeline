  -- Creates the curated (silver) layer table.
  -- Storing cleaned, typed, standardized data with derived columns. 

  USE DATABASE FLIGHT_PIPELINE_DB;                                                                                                                                                                 
  USE SCHEMA CURATED;

   CREATE OR REPLACE TABLE CLEAN_ROUTES (
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
    -- derived columns
    ROUTE_KEY               STRING,
    IS_INTERNATIONAL        BOOLEAN,
    DISTANCE_CATEGORY       STRING,
    -- metadata columns
    LOAD_TIMESTAMP          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE             STRING
  );