  -- 04_copy_into.sql                                                                                                                                                                      
  -- Loads the staged CSV file into the raw table.


  USE DATABASE FLIGHT_PIPELINE_DB;                                                                                                                                                         
  USE SCHEMA RAW; 
                                                                                                                                                                                           
  COPY INTO RAW_ROUTES (                  
    AIRLINE_CODE, AIRLINE_NAME, FLIGHT_NUMBER,                                                                                                                                             
    ORIGIN_AIRPORT, ORIGIN_CITY, ORIGIN_COUNTRY, ORIGIN_REGION,                                                                                                                            
    ORIGIN_LATITUDE, ORIGIN_LONGITUDE,    
    DESTINATION_AIRPORT, DESTINATION_CITY, DESTINATION_COUNTRY, DESTINATION_REGION,                                                                                                        
    DESTINATION_LATITUDE, DESTINATION_LONGITUDE,                                                                                                                                           
    DISTANCE_KM, SEATS, AIRCRAFT_TYPE, CODESHARE, STOPS,
    FLIGHT_DATE, FLIGHT_YEAR, FLIGHT_MONTH, FLIGHT_QUARTER,                                                                                                                                
    SOURCE_FILE                                                                                                                                                                            
  )                                                                                                                                                                                        
  FROM (                                                                                                                                                                                   
    SELECT                                                                                                                                                                                 
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,                                                                                                                    
      $16, $17, $18, $19, $20, $21, $22, $23, $24,                                                                                                                                         
      METADATA$FILENAME                       
    FROM @RAW_STAGE/routes_sample_data.csv    
  )                                                                                                                                                                                        
  FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)                                                                                                                                                 
  ON_ERROR = 'CONTINUE';    