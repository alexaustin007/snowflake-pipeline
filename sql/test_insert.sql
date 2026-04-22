  USE WAREHOUSE PIPELINE_WH;
  USE DATABASE FLIGHT_PIPELINE_DB;                                                                                                                                                                 
  USE SCHEMA RAW; 
                                                                                                                                                                                                   
  INSERT INTO RAW_ROUTES
    (AIRLINE_CODE, AIRLINE_NAME, FLIGHT_NUMBER,                                                                                                                                                    
     ORIGIN_AIRPORT, ORIGIN_CITY, ORIGIN_COUNTRY, ORIGIN_REGION,
     ORIGIN_LATITUDE, ORIGIN_LONGITUDE,                                                                                                                                                            
     DESTINATION_AIRPORT, DESTINATION_CITY, DESTINATION_COUNTRY, DESTINATION_REGION,                                                                                                               
     DESTINATION_LATITUDE, DESTINATION_LONGITUDE,                                                                                                                                                  
     DISTANCE_KM, SEATS, AIRCRAFT_TYPE, CODESHARE, STOPS,                                                                                                                                          
     FLIGHT_DATE, FLIGHT_YEAR, FLIGHT_MONTH, FLIGHT_QUARTER, SOURCE_FILE)                                                                                                                          
  VALUES                                                                                                                                                                                           
    ('AA', 'American Airlines', 'AA100',                                                                                                                                                           
     'JFK', 'New York', 'USA', 'North America',                                                                                                                                                    
     40.6413, -73.7781,
     'LHR', 'London', 'United Kingdom', 'Europe',                                                                                                                                                  
     51.4700, -0.4543,
     5541.0, 220, 'B77W', 0, 0,                                                                                                                                                                    
     '2026-04-21', 2026, 4, 2, 'manual_test_insert');