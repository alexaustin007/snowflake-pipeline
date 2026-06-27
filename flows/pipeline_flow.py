from prefect import flow, task, get_run_logger
import subprocess                                                                                                                                                                                
                
@task(                                                                                                                                                                                           
    retries=2,  
    retry_delay_seconds=30,                                                                                                                                                                      
    task_run_name="run_sql: {file_path}",
)                                                                                                                                                                                                
def run_sql(file_path: str):
    logger = get_run_logger()                                                                                                                                                                    
    logger.info(f"Running SQL file: {file_path}")
    subprocess.run(["snow", "sql", "-f", file_path], check=True)                                                                                                                                 
    logger.info(f"Finished SQL file: {file_path}")                                                                                                                                               
                                                                                                                                                                                                
@task(                                                                                                                                                                                           
    retries=3,  
    retry_delay_seconds=30,
    task_run_name="upload: {local_path}",                                                                                                                                                        
)
def upload_sample(local_path: str = "data/routes_sample_data.csv"):                                                                                                                              
    logger = get_run_logger()                                                                                                                                                                    
    logger.info(f"Uploading {local_path} to RAW_STAGE")
    subprocess.run([                                                                                                                                                                             
        "snow", "stage", "copy",
        local_path,                                                                                                                                                                              
        "@FLIGHT_PIPELINE_DB.RAW.RAW_STAGE",
        "--overwrite"                                                                                                                                                                            
    ], check=True)                                                                                                                                                                               
    logger.info("Upload complete.")                                                                                                                                                              
                                                                                                                                                                                                
@task(task_run_name="trigger: PROCESS_NEW_ROUTES")                                                                                                                                               
def trigger_processing():
    logger = get_run_logger()                                                                                                                                                                    
    logger.info("Executing CURATED.PROCESS_NEW_ROUTES task in Snowflake")                                                                                                                        
    subprocess.run([                                                                                                                                                                             
        "snow", "sql", "-q",                                                                                                                                                                     
        "EXECUTE TASK FLIGHT_PIPELINE_DB.CURATED.PROCESS_NEW_ROUTES;"                                                                                                                            
    ], check=True)                                                                                                                                                                               
    logger.info("Snowflake task triggered successfully.")                                                                                                                                        
                                                                                                                                                                                                                                                                      

@flow(name="flight_pipeline", flow_run_name="flight-pipeline-run")                                                                                                                               
def flight_pipeline():                                                                                                                                                                           
    logger = get_run_logger()
    logger.info("=== Starting flight routes pipeline ===")                                                                                                                                                                                       
    run_sql("sql/01_setup.sql")
    run_sql("sql/02_file_formats_stages.sql")
    run_sql("sql/03_raw_tables.sql")
    run_sql("sql/05_streams.sql")
    upload_sample()
    run_sql("sql/04_copy_into.sql")
    run_sql("sql/07_tasks.sql")
    trigger_processing()
    run_sql("sql/06_clean_tables.sql")
    run_sql("sql/08_cortex_enrichment.sql")
    run_sql("sql/09_final_views.sql")
    run_sql("sql/10_validation.sql")
                                                                                                                                                                                                
    logger.info("=== Pipeline complete ===")                                                                                                                                                          

if __name__ == "__main__":                                                                                                                                                                       
    flight_pipeline()