import pyspark.sql.functions as F
from processing.config.sparkconfig import spark


def join_df(
        avc_df:spark.createDataFrame,
        temp_df:spark.createDataFrame,
        prec_df:spark.createDataFrame,
        soil_df:spark.createDataFrame)->spark.createDataFrame:
    
    # join 1
    df = avc_df.join(temp_df.select(F.col("Municipio"), F.col("annual_avg_temp")), "Municipio", "inner")

    # join 2
    df1 = df.join(prec_df.select(F.col("Municipio"), F.col("annual_avg_preci_mm")), "Municipio", "inner")

    # join 3
    df2 = df1.join(soil_df, "Municipio", "inner")
    return df2

def save(df:spark.createDataFrame, path:str)->spark.createDataFrame:

    (
        df.write
        .partitionBy(
           "cultivo"
        )
        .mode("overwrite")
        #.parquet(path)
        .format("parquet")
        .save(path)
    )
