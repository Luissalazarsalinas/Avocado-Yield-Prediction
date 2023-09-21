import os
import findspark 
from pyspark.sql import SparkSession
from delta import *

# inti findspark
findspark.init("F:\Spark\spark")

# Spark Seccion config 
try:
    builder = (
        SparkSession.builder 
            .config("spark.sql.shuffle.partitions", "4") # partitions = 4
            .config("spark.executor.memory", "4g") # the executor memory is of 4 gb
            .config("spark.shuffle.file.buffer", "1MB") # this with 1mb of value allow spark to do more buffering before writing final map results to disk
            .config("spark.file.transferTo", "false") # this with false value decrease the I/O actitvity
            .config("spark.shuffle.unsafe.file.output.buffer", "1MB") # control the amount of buffering possible when mergin files during shuffle operations
            .config("spark.io.compression.lz4.blockSize", "512MB") # increase to 512mb to decreasse the size of the shuffle file by increasing the compressed size of the block
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") # config delta table into spark
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .appName("Data-Cleaning-and-Transformation")   
            .master("local[*]") 

            # .config("spark.hadoop.fs.defaultFS","hdfs://localhost:9000") \
            # .config("spark.hadoop.yarn.resourcemanager.address","localhost:8032") \
            # .config("spark.hadoop.mapreduce.framework.name","yarn") \
            # .config("spark.hadoop.yarn.submit.waitAppCompletion", "false") \
            
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
except Exception as e:
    print(f"Fail SparkSession creation: {e}")

