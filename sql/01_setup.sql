-- 01_setup.sql
  -- Creates the foundational Snowflake objects: warehouse, database, schemas
                                          
CREATE OR REPLACE WAREHOUSE PIPELINE_WH     
    WAREHOUSE_SIZE = 'XSMALL'                                                                                                                                                                      
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;                     
CREATE OR REPLACE DATABASE FLIGHT_PIPELINE_DB;  

CREATE OR REPLACE SCHEMA FLIGHT_PIPELINE_DB.RAW;  
CREATE OR REPLACE SCHEMA FLIGHT_PIPELINE_DB.CURATED;
CREATE OR REPLACE SCHEMA FLIGHT_PIPELINE_DB.AI;  