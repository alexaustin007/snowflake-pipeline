  -- Data quality / validation checks across the pipeline layers.
  -- Run after every deploy; log results to AUDIT_LOG for historical tracking.                                                                                                          


  USE DATABASE FLIGHT_PIPELINE_DB;                                                                                                                                                      
  USE SCHEMA CURATED;  

  -- Audit log: records each validation check run with pass/fail + row counts                                                                                                           
  CREATE TABLE IF NOT EXISTS AUDIT_LOG (
    CHECK_ID          STRING,                                                                                                                                                           
    CHECK_NAME        STRING,                                                                                                                                                         
    LAYER             STRING,                                                                                                                                                           
    RESULT            STRING,        -- 'PASS' or 'FAIL'                                                                                                                                
    ACTUAL_COUNT      NUMBER,                                                                                                                                                           
    EXPECTED_MIN      NUMBER,                                                                                                                                                           
    EXPECTED_MAX      NUMBER,                                                                                                                                                           
    MESSAGE           STRING,                                                                                                                                                           
    RUN_AT            TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
  );                                
                                                                                                                     
  -- 1. COMPLETENESS CHECKS                                                                                                                                                                                                                                                                           
   
  -- Raw table should have rows                                                                                                                                                         
  SELECT                                                                                                                                                                              
    'CHK_RAW_NOT_EMPTY'         AS CHECK_ID,                                                                                                                                            
    'Raw table has rows'        AS CHECK_NAME,                                                                                                                                          
    COUNT(*)                    AS ROW_COUNT,                                                                                                                                           
    IFF(COUNT(*) > 0, 'PASS', 'FAIL') AS RESULT                                                                                                                                         
  FROM RAW.RAW_ROUTES;                                                                                                                                                                  
                                                                                                                                                                                        
  -- Curated should have rows                                                                                                                                                           
  SELECT                                                                                                                                                                              
    'CHK_CLEAN_NOT_EMPTY'       AS CHECK_ID,
    'Clean table has rows'      AS CHECK_NAME,                                                                                                                                          
    COUNT(*)                    AS ROW_COUNT,
    IFF(COUNT(*) > 0, 'PASS', 'FAIL') AS RESULT                                                                                                                                         
  FROM CURATED.CLEAN_ROUTES;                                                                                                                                                          
                                                                                                                                                                                        
  -- No nulls in critical columns (curated)                                                                                                                                           
  SELECT                                                                                                                                                                                
    'CHK_NO_NULL_KEYS'          AS CHECK_ID,                                                                                                                                            
    'Curated: no nulls in keys' AS CHECK_NAME,
    COUNT(*)                    AS NULL_COUNT,                                                                                                                                          
    IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS RESULT                                                                                                                                       
  FROM CURATED.CLEAN_ROUTES                                                                                                                                                             
  WHERE AIRLINE_CODE IS NULL                                                                                                                                                          
     OR FLIGHT_DATE IS NULL                                                                                                                                                             
     OR ORIGIN_AIRPORT IS NULL                                                                                                                                                        
     OR DESTINATION_AIRPORT IS NULL;             
                                                                                                                  
  -- 2. UNIQUENESS CHECKS                                                                                                                                                               

  -- No exact duplicate rows in curated                                                                                                                                                 
  SELECT
    'CHK_NO_DUPLICATES'                  AS CHECK_ID,                                                                                                                                   
    'Curated: no duplicate flight rows'  AS CHECK_NAME,                                                                                                                               
    COUNT(*) - COUNT(DISTINCT AIRLINE_CODE, FLIGHT_NUMBER, FLIGHT_DATE, ORIGIN_AIRPORT, DESTINATION_AIRPORT) AS DUPLICATES,                                                             
    IFF(COUNT(*) = COUNT(DISTINCT AIRLINE_CODE, FLIGHT_NUMBER, FLIGHT_DATE, ORIGIN_AIRPORT, DESTINATION_AIRPORT), 'PASS', 'FAIL') AS RESULT                                             
  FROM CURATED.CLEAN_ROUTES;                    
                                                                                                            
  -- 3. VALIDITY CHECKS                                                                                                                                                               
                                                                                                                                                                                                                                                                                                   
  -- Distance should be positive and reasonable (< 20000 km — longest nonstop is ~15000)                                                                                                
  SELECT
    'CHK_DISTANCE_VALID'              AS CHECK_ID,                                                                                                                                      
    'Distance within 0–20000 km'      AS CHECK_NAME,                                                                                                                                    
    COUNT(*)                          AS INVALID_COUNT,
    IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS RESULT                                                                                                                                         
  FROM CURATED.CLEAN_ROUTES                                                                                                                                                             
  WHERE DISTANCE_KM <= 0 OR DISTANCE_KM > 20000;                                                                                                                                        
                                                                                                                                                                                        
  -- Flight dates should be in a sane range (1970–2030)                                                                                                                                 
  SELECT                                                                                                                                                                                
    'CHK_FLIGHT_DATE_SANE'            AS CHECK_ID,                                                                                                                                      
    'Flight dates between 1970–2030'  AS CHECK_NAME,                                                                                                                                  
    COUNT(*)                          AS INVALID_COUNT,                                                                                                                                 
    IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS RESULT
  FROM CURATED.CLEAN_ROUTES                                                                                                                                                             
  WHERE FLIGHT_DATE < '1970-01-01' OR FLIGHT_DATE > '2030-12-31';                                                                                                                     
                                                                                                                                                                                        
  -- Latitude/longitude within valid geographic ranges                                                                                                                                  
  SELECT                                                                                                                                                                                
    'CHK_COORDINATES_VALID'           AS CHECK_ID,                                                                                                                                      
    'Origin/dest coordinates valid'   AS CHECK_NAME,                                                                                                                                  
    COUNT(*)                          AS INVALID_COUNT,                                                                                                                                 
    IFF(COUNT(*) = 0, 'PASS', 'FAIL') AS RESULT
  FROM CURATED.CLEAN_ROUTES                                                                                                                                                             
  WHERE ORIGIN_LATITUDE NOT BETWEEN -90 AND 90                                                                                                                                        
     OR DESTINATION_LATITUDE NOT BETWEEN -90 AND 90                                                                                                                                     
     OR ORIGIN_LONGITUDE NOT BETWEEN -180 AND 180                                                                                                                                       
     OR DESTINATION_LONGITUDE NOT BETWEEN -180 AND 180;                                                                                                                                 
                                                                                                                                                                                                                                                                                                        
  -- 4. CONSISTENCY CHECKS                                                                                                                                                              
                                                                                                                   

  -- Every curated row should trace back to a raw row on AIRLINE_CODE+FLIGHT_NUMBER+FLIGHT_DATE                                                                                         
  SELECT
    'CHK_CURATED_TRACES_TO_RAW'           AS CHECK_ID,                                                                                                                                  
    'All curated rows traceable to raw'   AS CHECK_NAME,                                                                                                                              
    COUNT(*)                              AS ORPHAN_COUNT,                                                                                                                              
    IFF(COUNT(*) = 0, 'PASS', 'FAIL')     AS RESULT                                                                                                                                     
  FROM CURATED.CLEAN_ROUTES c                                                                                                                                                           
  WHERE NOT EXISTS (                                                                                                                                                                    
    SELECT 1 FROM RAW.RAW_ROUTES r                                                                                                                                                    
    WHERE UPPER(TRIM(r.AIRLINE_CODE)) = c.AIRLINE_CODE                                                                                                                                  
      AND UPPER(TRIM(r.FLIGHT_NUMBER)) = c.FLIGHT_NUMBER                                                                                                                                
      AND r.FLIGHT_DATE = c.FLIGHT_DATE                                                                                                                                                 
  );                                                                                                                                                                                    
                                                                                                                                                                                      
  -- IS_INTERNATIONAL logic check — countries must actually differ when flag is TRUE                                                                                                    
  SELECT                                                                                                                                                                              
    'CHK_INTERNATIONAL_LOGIC'             AS CHECK_ID,                                                                                                                                  
    'IS_INTERNATIONAL matches country diff' AS CHECK_NAME,                                                                                                                            
    COUNT(*)                              AS MISMATCH_COUNT,                                                                                                                            
    IFF(COUNT(*) = 0, 'PASS', 'FAIL')     AS RESULT
  FROM CURATED.CLEAN_ROUTES                                                                                                                                                             
  WHERE (IS_INTERNATIONAL = TRUE  AND TRIM(ORIGIN_COUNTRY) = TRIM(DESTINATION_COUNTRY))                                                                                               
     OR (IS_INTERNATIONAL = FALSE AND TRIM(ORIGIN_COUNTRY) <> TRIM(DESTINATION_COUNTRY));                                                                                               
                                                                                                            
  -- 5. LOG ALL RESULTS TO AUDIT_LOG (single insert pattern)                                                                                                                            
                                                                                                                     
                                                                                                                                                                                        
  INSERT INTO CURATED.AUDIT_LOG (CHECK_ID, CHECK_NAME, LAYER, RESULT, ACTUAL_COUNT, MESSAGE)                                                                                            
  SELECT 'CHK_RAW_NOT_EMPTY',  'Raw table has rows',        'RAW',                                                                                                                      
         IFF(COUNT(*) > 0, 'PASS', 'FAIL'), COUNT(*),                                                                                                                                   
         'Expected > 0 rows'                                                                                                                                                            
  FROM RAW.RAW_ROUTES;                                                                                                                                                                  
                                                                                                                                                                                        
  INSERT INTO CURATED.AUDIT_LOG (CHECK_ID, CHECK_NAME, LAYER, RESULT, ACTUAL_COUNT, MESSAGE)                                                                                            
  SELECT 'CHK_CLEAN_NOT_EMPTY', 'Clean table has rows',     'CURATED',
         IFF(COUNT(*) > 0, 'PASS', 'FAIL'), COUNT(*),                                                                                                                                   
         'Expected > 0 rows'                                                                                                                                                            
  FROM CURATED.CLEAN_ROUTES;                                                                                                                                                            
                                                                                                                                                                                        
  INSERT INTO CURATED.AUDIT_LOG (CHECK_ID, CHECK_NAME, LAYER, RESULT, ACTUAL_COUNT, MESSAGE)                                                                                            
  SELECT 'CHK_NO_NULL_KEYS', 'Curated: no nulls in keys',   'CURATED',
         IFF(COUNT(*) = 0, 'PASS', 'FAIL'), COUNT(*),                                                                                                                                   
         'Expected 0 null-key rows'                                                                                                                                                     
  FROM CURATED.CLEAN_ROUTES                                                                                                                                                             
  WHERE AIRLINE_CODE IS NULL OR FLIGHT_DATE IS NULL                                                                                                                                     
     OR ORIGIN_AIRPORT IS NULL OR DESTINATION_AIRPORT IS NULL;                                                                                                                        
                                                                                                                                                                                        
  -- Final summary view of most recent run                                                                                                                                              
  SELECT CHECK_ID, CHECK_NAME, LAYER, RESULT, ACTUAL_COUNT, RUN_AT                                                                                                                      
  FROM CURATED.AUDIT_LOG                                                                                                                                                                
  WHERE RUN_AT > DATEADD('minute', -5, CURRENT_TIMESTAMP())                                                                                                                           
  ORDER BY RUN_AT DESC;                