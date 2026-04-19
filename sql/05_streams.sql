                                                                                                                                                                              
  -- Creates a stream on RAW_ROUTES for change data capture (CDC).
  -- Stream tracks new rows inserted after its creation                                                                                                                                             

  USE DATABASE FLIGHT_PIPELINE_DB;                                                                                                                                                                 
  USE SCHEMA RAW; 

CREATE OR REPLACE STREAM RAW_ROUTES_STREAM                                                                                                                                                       
  ON TABLE RAW_ROUTES
  APPEND_ONLY = TRUE; 

   SHOW STREAMS IN SCHEMA RAW;