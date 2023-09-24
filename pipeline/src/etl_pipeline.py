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
defaul_arg = ...