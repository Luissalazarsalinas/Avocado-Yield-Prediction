import findspark 
from pyspark.sql import SparkSession

# inti findspark
findspark.init("D:\Spark\spark")

# Spark Seccion config 
try:
    spark = (
        SparkSession.builder \
            .appName("Data-Cleaning-and-Transformation") \
            #.master("local[*]") \
            # .config("spark.hadoop.fs.defaultFS","hdfs://localhost:9000") \
            # .config("spark.hadoop.yarn.resourcemanager.address","localhost:8032") \
            # .config("spark.hadoop.mapreduce.framework.name","yarn") \
            # .config("spark.hadoop.yarn.submit.waitAppCompletion", "false") \
            .getOrCreate()
    )
except Exception as e:
    print(f"Fail SparkSession creation: {e}")

