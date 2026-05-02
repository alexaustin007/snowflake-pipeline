from prefect import flow, task                                                                                                                                                                   
import subprocess

@task(retries=2, retry_delay_seconds=30)                                                                                                                                                         
def run_sql(file_path: str):
    subprocess.run(["snow", "sql", "-f", file_path], check=True)                                                                                                                                 
                                                                                                                                                                                                
@task(retries=3, retry_delay_seconds=30)
def upload_sample():                                                                                                                                                                             
    subprocess.run([                                                                                                                                                                             
        "snow", "stage", "copy",
        "data/routes_sample_data.csv",                                                                                                                                                           
        "@FLIGHT_PIPELINE_DB.RAW.RAW_STAGE",                                                                                                                                                     
        "--overwrite"                                                                                                                                                                            
    ], check=True)                                                                                                                                                                               
                                                                                                                                                                                                
@task                                                                                                                                                                                            
def trigger_processing():
    subprocess.run([
        "snow", "sql", "-q",
        "EXECUTE TASK FLIGHT_PIPELINE_DB.CURATED.PROCESS_NEW_ROUTES;"                                                                                                                            
    ], check=True)                                                                                                                                                                               
                                                                                                                                                                                                
@flow(name="flight_pipeline")                                                                                                                                                                    
def flight_pipeline():
    run_sql("sql/01_setup.sql")
    run_sql("sql/02_file_formats_stages.sql")                                                                                                                                                    
    run_sql("sql/03_raw_tables.sql")
    upload_sample()                                                                                                                                                                              
    run_sql("sql/04_copy_into.sql")
    run_sql("sql/05_streams.sql")                                                                                                                                                                
    run_sql("sql/06_clean_tables.sql")
    run_sql("sql/07_tasks.sql")                                                                                                                                                                  
    trigger_processing()
    run_sql("sql/08_cortex_enrichment.sql")                                                                                                                                                      
    run_sql("sql/09_final_views.sql")
    run_sql("sql/10_validation.sql")                                                                                                                                                             

if __name__ == "__main__":                                                                                                                                                                       
    flight_pipeline()