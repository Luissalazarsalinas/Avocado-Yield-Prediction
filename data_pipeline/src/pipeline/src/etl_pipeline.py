import os
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ingest.src.ingest_data import ingest
from processing.src.curate import cuarate_data
from processing.src.avocado_clean import clean_avacado_data
from processing.src.temp_clean import clean_temp_data
from processing.src.prec_clean import clean_precipitation_data
from processing.src.soil_clean import clean_soil_data
from load_data.src.Ml_data import ml_data
from load_data.src.load_datawarehouse import load_datawarehouse

# default dag arguments
defaul_arg = {
    "owner": "Data Engineering Team",
    #"dependes_on_past":False,
    "start_date": datetime(2023,9,26), # the pipeline star run in this data
    #"email": [""], #
    "email_on_failure":True,  # send email if exist fails 
    "email_on_retry":True,    # send email to retry
    "retries": 3, # three retries 
    "retry_delay": timedelta(minutes=3), # The time among retries is 3 minutes
    # "on_failure_callback":f_callback,  
    # "on_success_callback":s_callback,
    # "on_retry_callback":r_callback

}

# DAG and tasks definitions
with DAG(
    dag_id="dag_avocado_elt_pipeline",
    default_args= defaul_arg,
    schedule_interval= "30 8 * * tue", # the pipeline run the November 26th-tuesday of 2023 at 8:30 am
    catchup= True


) as dag:
    
    # ## Task - start hadoop hdfs
    # if os.path.exists():
    #     hadoop = BashOperator(
    #         task_id = "Start Hadoop HDFS",
    #         bash_command= "path where is the shell script file"
    #         ) 
    # else :
    #     raise Exception(f"Cannot Locate {dag} file")
    

    ## Task - Cleaning data
    ingest_data = PythonOperator(
        task_id = "ingest_data_to_raw_zone",
        python_callable= ingest
    )
    
    ## Task - Cleaning data
    avocado_clean = PythonOperator(
        task_id = "clean_avocado_yield_data",
        python_callable= clean_avacado_data
    )

    ## Task - Cleaning data
    temp_clean = PythonOperator(
        task_id = "clean_temperature_data",
        python_callable= clean_temp_data
    )

    ## Task - Cleaning data
    prec_clean = PythonOperator(
        task_id = "clean_prec_data",
        python_callable= clean_precipitation_data
    )

    ## Task - Cleaning data
    soil_clean = PythonOperator(
        task_id = "clean_soil_data",
        python_callable= clean_soil_data
    )

    ## Task - Curate data more refine transformations
    cur_data = PythonOperator(
        task_id = "curate_data",
        python_callable= cuarate_data
    )

    ## Task - Load data into a Datawaehouse
    load_dwh = PythonOperator(
        task_id = "load_datawarehouse",
        python_callable= load_datawarehouse
    )

    ## Task - Extract data for ML process 
    Ml_data = PythonOperator(
        task_id = "Ml_data",
        python_callable= ml_data
    )


    # Dependecies
    ingest_data >> [avocado_clean,temp_clean, prec_clean, soil_clean] >> cur_data >> [load_dwh, Ml_data]
