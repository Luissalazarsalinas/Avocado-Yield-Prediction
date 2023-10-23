import pyspark.sql.functions as F
from processing.config.sparkconfig import spark


class TempClean:

    def modify_columns(self, data:spark.createDataFrame) ->spark.createDataFrame:
        
        df = (
            data
            .withColumnRenamed("CodigoEstacion", "cod_estacion")
            .withColumnRenamed("CodigoSensor", "cod_sensor")
            .withColumnRenamed("ValorObservado", "Temp_gC")
            .withColumn("FechaObservacion", F.to_date("FechaObservacion", "MM/dd/yyyy hh:mm:ss a"))
            .withColumn("year_temp", F.year("FechaObservacion"))
            .withColumn("Mes_temp", F.month("FechaObservacion"))
            .withColumn("Departamento", F.lower(F.col("Departamento")))
            .withColumn("Municipio", F.lower(F.col("Municipio")))
            .drop(F.col("FechaObservacion"))
            .drop(F.col("NombreEstacion"))
            .drop(F.col("ZonaHidrografica"))
            .drop(F.col("DescripcionSensor"))
            .drop(F.col("UnidadMedida"))
        )
        return df

    def clean_data_temp(self, data:spark.createDataFrame) ->spark.createDataFrame:

        df = (
            data
            .dropDuplicates()
            .dropna()
            .filter(~((data["Temp_gC"] == 0))) # Drop columns with precipitations values == to zero
            .orderBy(F.col("Municipio").asc())
        )

        return df
    
    def union(self,
         data:spark.createDataFrame,
         data1:spark.createDataFrame,
         data2:spark.createDataFrame) ->spark.createDataFrame:
        
        df_union = (
            data
            .union(data1)
            .union(data2)
        )

        return df_union
    
    def aggregation(self, data:spark.createDataFrame) ->spark.createDataFrame:

        df = (
            data
            .groupBy("Departamento","Municipio", "Mes_temp", "year_temp")
            .avg("Temp_gC")
            .withColumnRenamed("avg(Temp_gC)", "annual_avg_temp")
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
