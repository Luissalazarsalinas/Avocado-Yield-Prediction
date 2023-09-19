import pyspark.sql.functions as F
from config.sparkconfig import spark



class AvocadoClean:
                
    
    def modify_column(self, data:spark.createDataFrame)-> spark.createDataFrame:

        df = (
            data
            .withColumnRenamed("CÓD. DEP.", "cod_dep")
            .withColumnRenamed("DEPARTAMENTO", "Departamento")
            .withColumnRenamed("CÓD. MUN.", "cod_mun")
            .withColumnRenamed("MUNICIPIO", "Municipio")
            .withColumnRenamed("GRUPO DE CULTIVO", "grupo_cultivo")
            .withColumnRenamed("SUBGRUPO DE CULTIVO", "subgrupo_cultivo")
            .withColumnRenamed("CULTIVO", "cultivo")
            .withColumnRenamed("DESAGREGACIÓN REGIONAL Y/O SISTEMA PRODUCTIVO", "sistema_productivo")
            .withColumnRenamed("AÑO", "year")
            .withColumnRenamed("PERIODO", "periodo")
            .withColumnRenamed("Área Sembrada (ha)", "area_sembrada_ha")
            .withColumnRenamed("Área Cosechada (ha)", "area_cosechada_ha")
            .withColumnRenamed("Producción (t)", "produccion_t")
            .withColumnRenamed("Rendimiento (t/ha)", "rendimiento_t_ha")
            .withColumnRenamed("ESTADO FISICO PRODUCCION", "estado_fisico_prod")
            .withColumnRenamed("NOMBRE CIENTIFICO", "nombre_cientifico")
            .withColumnRenamed("CICLO DE CULTIVO", "ciclo_cultivo")
            .withColumn("Departamento", F.lower(F.col("Departamento")))
            .withColumn("Municipio", F.lower(F.col("Municipio")))
            .withColumn("grupo_cultivo", F.lower(F.col("grupo_cultivo")))
            .withColumn("subgrupo_cultivo", F.lower(F.col("subgrupo_cultivo")))
            .withColumn("cultivo", F.lower(F.col("cultivo")))
            .withColumn("sistema_productivo", F.lower(F.col("sistema_productivo")))
            .withColumn("estado_fisico_prod", F.lower(F.col("estado_fisico_prod")))
            .withColumn("nombre_cientifico", F.lower(F.col("nombre_cientifico")))
            .withColumn("ciclo_cultivo", F.lower(F.col("ciclo_cultivo")))
            )
        
        return df
    
    def cleaning(self,data:spark.createDataFrame)-> spark.createDataFrame:

        df = (
            data
            .dropDuplicates()
            .dropna()
            .filter(~((data["area_sembrada_ha"] == 0) & (data["area_sembrada_ha"] == 0) & (data["produccion_t"] == 0)))
            .filter(~((data["area_sembrada_ha"] == 0) | (data["area_sembrada_ha"] == 0)))
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

