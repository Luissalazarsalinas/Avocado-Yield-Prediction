import pyspark.sql.functions as F
from processing.config.sparkconfig import spark

##
class PrecipClean:
    
    def modify_columns(self,data:spark.createDataFrame) ->spark.createDataFrame:
        
        df = (
            data
            .withColumnRenamed("CodigoEstacion", "cod_estacion")
            .withColumnRenamed("CodigoSensor", "cod_sensor")
            .withColumnRenamed("ValorObservado", "precipitaciones_mm")
            .withColumn("FechaObservacion", F.to_date("FechaObservacion", "MM/dd/yyyy hh:mm:ss a"))
            .withColumn("year_prec", F.year("FechaObservacion"))
            .withColumn("Mes_prec", F.month("FechaObservacion"))
            .withColumn("Departamento", F.lower(F.col("Departamento")))
            .withColumn("Municipio", F.lower(F.col("Municipio")))
            .drop(F.col("FechaObservacion"))
            .drop(F.col("NombreEstacion"))
            .drop(F.col("ZonaHidrografica"))
            .drop(F.col("DescripcionSensor"))
            .drop(F.col("UnidadMedida"))
        )
        return df

    def clean_data(self,data:spark.createDataFrame) ->spark.createDataFrame:

        df = (
            data
            .dropDuplicates()
            .dropna()
            .filter(~((data["precipitaciones_mm"] == 0))) # Drop columns with precipitations values == to zero
            )

        return df
    
    def union(self,
         data:spark.createDataFrame,
         data1:spark.createDataFrame,
         data2:spark.createDataFrame,
         data3:spark.createDataFrame,
         data4:spark.createDataFrame,
         data5:spark.createDataFrame,
         data6:spark.createDataFrame) ->spark.createDataFrame:
        
        df_union = (
            data
            .union(data1)
            .union(data2)
            .union(data3)
            .union(data4)
            .union(data5)
            .union(data6)
        )

        return df_union
    
    def aggregation(self, data:spark.createDataFrame) ->spark.createDataFrame:

        df = (
            data
            .groupBy("Departamento","Municipio", "Mes_prec", "year_prec")
            .avg("precipitaciones_mm")
            .withColumnRenamed("avg(precipitaciones_mm)", "annual_avg_preci_mm")
            .orderBy(F.col("Municipio").asc())
            )
        
        return df
    
    def save(self, data:spark.createDataFrame, path: str):

        (
            data.write
            .option("compression", "snappy")
            .mode("overwrite")
            .parquet(path)
        )