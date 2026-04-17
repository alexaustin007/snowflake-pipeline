  -- 02_file_formats_stages.sql
  -- Creates the CSV file format and internal stage for loading data

USE DATABASE FLIGHT_PIPELINE_DB; 
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null');

CREATE OR REPLACE STAGE RAW_STAGE
  FILE_FORMAT = CSV_FORMAT;
