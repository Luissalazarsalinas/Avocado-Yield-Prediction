import findspark 
from pathlib import Path 
from pyspark.sql import SparkSession
from delta import *

# Base path
BASE_PATH = Path(__file__).resolve(strict=True).parent

# inti findspark
findspark.init("D:\Spark\spark")

# Spark Seccion config 
try:
    builder = (
        SparkSession.builder 
            .config("spark.sql.shuffle.partitions", "4") # partitions = 4
            .config("spark.executor.memory", "4g") # the executor memory is of 4 gb
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") # config delta table into spark
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars", f"{BASE_PATH}\postgresql-42.6.0.jar") # config connection with postgrestSQL - JDBC
            .appName("Data-Cleaning-and-Transformation")   
            #.master("local[*]") 

            # .config("spark.hadoop.fs.defaultFS","hdfs://localhost:9000") \
            # .config("spark.hadoop.yarn.resourcemanager.address","localhost:8032") \
            # .config("spark.hadoop.mapreduce.framework.name","yarn") \
            # .config("spark.hadoop.yarn.submit.waitAppCompletion", "false") \       
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
except Exception as e:
    print(f"Fail SparkSession creation: {e}")

