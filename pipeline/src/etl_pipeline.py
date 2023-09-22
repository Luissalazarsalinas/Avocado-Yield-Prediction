from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processing.src import curate
from processing.src import avocado_clean, prec_clean, temp_clean, soil_clean
from load_data.src import Ml_data, load_datawarehouse

## Next task 
## configure the connection between spark and potsgrest
## Create funtion into all modules for processing and load 