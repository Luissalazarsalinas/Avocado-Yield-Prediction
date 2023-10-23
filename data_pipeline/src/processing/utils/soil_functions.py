import pyspark.sql.functions as F
from pyspark.sql.types import *
from processing.config.sparkconfig import spark

class SoilClean:

    def modify_columns(self, data:spark.createDataFrame) ->spark.createDataFrame:

        df = (
            data
            #.withColumnRenamed("Cultivo", "cultivo")
            .withColumnRenamed("pH agua:suelo 2,5:1,0", "ph_agua_suelo_2_5_1_0")
            .withColumnRenamed("Materia orgánica (MO) %", "materia_orgnica_mo_porcent")
            .withColumnRenamed("Fósforo (P) Bray II mg/kg","fosforo_p_bray_ii_mg_kg")
            .withColumnRenamed("Azufre (S) Fosfato monocalcico mg/kg", "azufres_fosfato_monocalcico_mg_kg")
            .withColumnRenamed("Calcio (Ca) intercambiable cmol(+)/kg", "calcio_intercambiable_cmol_kg")
            .withColumnRenamed("Magnesio (Mg) intercambiable cmol(+)/kg", "magnesio_mg_intercambiable_cmol_kg")
            .withColumnRenamed("Potasio (K) intercambiable cmol(+)/kg", "potasio_intercambiable_cmol_kg")
            .withColumnRenamed("Sodio (Na) intercambiable cmol(+)/kg", "sodio_intercambiable_cmol_kg")
            .withColumnRenamed("capacidad de intercambio cationico (CICE) suma de bases cmol(+)/kg","capacidad_intercambio_cationico_suma_de_bases_cmol_kg")
            .withColumnRenamed("Conductividad el‚ctrica (CE) relacion 2,5:1,0 dS/m", "conductividad_elctrica_relacion_2_5_1_0_ds_m")
            .withColumnRenamed("Hierro (Fe) disponible olsen mg/kg", "hierro_disponible_olsen_mg_kg")
            .withColumnRenamed("Cobre (Cu) disponible mg/kg", "cobre_disponible_mg_kg")
            .withColumnRenamed("Manganeso (Mn) disponible Olsen mg/kg", "manganeso_disponible_olsen_mg_kg")
            .withColumnRenamed("Zinc (Zn) disponible Olsen mg/kg", "zinc_disponible_olsen_mg_kg")
            .withColumnRenamed("Boro (B) disponible mg/kg", "boro_disponible_mg_kg")
            #.withColumn("Departamento", F.lower(F.col("Departamento")))
            .withColumn("Municipio", F.lower(F.col("Municipio")))
            #.withColumn("cultivo", F.lower(F.col("cultivo")))
            .withColumn("Topografia", F.lower(F.col("Topografia")))
            .withColumn("Drenaje", F.lower(F.col("Drenaje")))
        )
        return df

    def drop_columns(self, data:spark.createDataFrame) ->spark.createDataFrame:

        df = (
            data
            .drop(F.col("numfila"))
            .drop(F.col("Departamento"))
            .drop(F.col("Cultivo"))
            .drop(F.col("Estado"))
            .drop(F.col("Tiempo Establecimiento"))
            .drop(F.col("Riego"))
            .drop(F.col("Fertilizantes aplicados"))
            .drop(F.col("FechaAnalisis"))
            .drop(F.col("Acidez (Al+H) KCL cmol(+)/kg"))
            .drop(F.col("Aluminio (Al) intercambiable cmol(+)/kg"))
            .drop(F.col("Hierro (Fe) disponible doble  cido mg/kg"))
            .drop(F.col("Cobre (Cu) disponible doble acido mg/kg"))
            .drop(F.col("Manganeso (Mn) disponible doble acido mg/kg"))
            .drop(F.col("Zinc (Zn) disponible doble  cido mg/kg"))
            .drop(F.col("Secuencial"))
            
            )
        
        return df
    
    def replace_values(self, data:spark.createDataFrame) ->spark.createDataFrame:

        df = (
            data
            .withColumn("ph_agua_suelo_2_5_1_0", F.regexp_replace("ph_agua_suelo_2_5_1_0", "<", ""))
            .withColumn("materia_orgnica_mo_porcent", F.regexp_replace("materia_orgnica_mo_porcent", "<", ""))
            .withColumn("fosforo_p_bray_ii_mg_kg", F.regexp_replace("fosforo_p_bray_ii_mg_kg", "<", ""))
            .withColumn("azufres_fosfato_monocalcico_mg_kg", F.regexp_replace("azufres_fosfato_monocalcico_mg_kg", "<", ""))
            .withColumn("calcio_intercambiable_cmol_kg", F.regexp_replace("calcio_intercambiable_cmol_kg", "<", ""))
            .withColumn("magnesio_mg_intercambiable_cmol_kg", F.regexp_replace("magnesio_mg_intercambiable_cmol_kg", "<", ""))
            .withColumn("potasio_intercambiable_cmol_kg", F.regexp_replace("potasio_intercambiable_cmol_kg", "<", ""))
            .withColumn("sodio_intercambiable_cmol_kg", F.regexp_replace("sodio_intercambiable_cmol_kg", "<", ""))
            .withColumn("capacidad_intercambio_cationico_suma_de_bases_cmol_kg", F.regexp_replace("capacidad_intercambio_cationico_suma_de_bases_cmol_kg", "<", ""))
            .withColumn("conductividad_elctrica_relacion_2_5_1_0_ds_m", F.regexp_replace("conductividad_elctrica_relacion_2_5_1_0_ds_m", "<", ""))
            .withColumn("hierro_disponible_olsen_mg_kg", F.regexp_replace("hierro_disponible_olsen_mg_kg", "<", ""))
            .withColumn("cobre_disponible_mg_kg", F.regexp_replace("cobre_disponible_mg_kg", "<", ""))
            .withColumn("manganeso_disponible_olsen_mg_kg", F.regexp_replace("manganeso_disponible_olsen_mg_kg", "<", ""))
            .withColumn("zinc_disponible_olsen_mg_kg", F.regexp_replace("zinc_disponible_olsen_mg_kg", "<", ""))
            .withColumn("boro_disponible_mg_kg", F.regexp_replace("boro_disponible_mg_kg", "<", ""))
        )
        
        df_n = (
            df
            .withColumn("ph_agua_suelo_2_5_1_0", F.regexp_replace("ph_agua_suelo_2_5_1_0",",", "."))
            .withColumn("materia_orgnica_mo_porcent", F.regexp_replace("materia_orgnica_mo_porcent", ",", "."))
            .withColumn("fosforo_p_bray_ii_mg_kg", F.regexp_replace("fosforo_p_bray_ii_mg_kg", ",", "."))
            .withColumn("azufres_fosfato_monocalcico_mg_kg", F.regexp_replace("azufres_fosfato_monocalcico_mg_kg", ",", "."))
            .withColumn("calcio_intercambiable_cmol_kg", F.regexp_replace("calcio_intercambiable_cmol_kg", ",", "."))
            .withColumn("magnesio_mg_intercambiable_cmol_kg", F.regexp_replace("magnesio_mg_intercambiable_cmol_kg", ",", "."))
            .withColumn("potasio_intercambiable_cmol_kg", F.regexp_replace("potasio_intercambiable_cmol_kg", ",", "."))
            .withColumn("sodio_intercambiable_cmol_kg", F.regexp_replace("sodio_intercambiable_cmol_kg", ",", "."))
            .withColumn("capacidad_intercambio_cationico_suma_de_bases_cmol_kg", F.regexp_replace("capacidad_intercambio_cationico_suma_de_bases_cmol_kg", ",", "."))
            .withColumn("conductividad_elctrica_relacion_2_5_1_0_ds_m", F.regexp_replace("conductividad_elctrica_relacion_2_5_1_0_ds_m", ",", "."))
            .withColumn("hierro_disponible_olsen_mg_kg", F.regexp_replace("hierro_disponible_olsen_mg_kg", ",", "."))
            .withColumn("cobre_disponible_mg_kg", F.regexp_replace("cobre_disponible_mg_kg", ",", "."))
            .withColumn("manganeso_disponible_olsen_mg_kg", F.regexp_replace("manganeso_disponible_olsen_mg_kg", ",", "."))
            .withColumn("zinc_disponible_olsen_mg_kg", F.regexp_replace("zinc_disponible_olsen_mg_kg", ",", "."))
            .withColumn("boro_disponible_mg_kg", F.regexp_replace("boro_disponible_mg_kg", ",", "."))
            
            )
        
        df_n = (
            df
            .withColumn("ph_agua_suelo_2_5_1_0", F.col("ph_agua_suelo_2_5_1_0").cast(DecimalType(10,2)))
            .withColumn("materia_orgnica_mo_porcent", F.col("materia_orgnica_mo_porcent").cast(DecimalType(10,2)))
            .withColumn("fosforo_p_bray_ii_mg_kg", F.col("fosforo_p_bray_ii_mg_kg").cast(DecimalType(10,2)))
            .withColumn("azufres_fosfato_monocalcico_mg_kg", F.col("azufres_fosfato_monocalcico_mg_kg").cast(DecimalType(10,2)))
            .withColumn("calcio_intercambiable_cmol_kg", F.col("calcio_intercambiable_cmol_kg").cast(DecimalType(10,2)))
            .withColumn("magnesio_mg_intercambiable_cmol_kg", F.col("magnesio_mg_intercambiable_cmol_kg").cast(DecimalType(10,2)))
            .withColumn("potasio_intercambiable_cmol_kg", F.col("potasio_intercambiable_cmol_kg").cast(DecimalType(10,2)))
            .withColumn("sodio_intercambiable_cmol_kg", F.col("sodio_intercambiable_cmol_kg").cast(DecimalType(10,2)))
            .withColumn("capacidad_intercambio_cationico_suma_de_bases_cmol_kg", F.col("capacidad_intercambio_cationico_suma_de_bases_cmol_kg").cast(DecimalType(10,2)))
            .withColumn("conductividad_elctrica_relacion_2_5_1_0_ds_m", F.col("conductividad_elctrica_relacion_2_5_1_0_ds_m").cast(DecimalType(10,2)))
            .withColumn("hierro_disponible_olsen_mg_kg", F.col("hierro_disponible_olsen_mg_kg").cast(DecimalType(10,4)))
            .withColumn("cobre_disponible_mg_kg", F.col("cobre_disponible_mg_kg").cast(DecimalType(10,4)))
            .withColumn("manganeso_disponible_olsen_mg_kg", F.col("manganeso_disponible_olsen_mg_kg").cast(DecimalType(10,4)))
            .withColumn("zinc_disponible_olsen_mg_kg", F.col("zinc_disponible_olsen_mg_kg").cast(DecimalType(10,4)))
            .withColumn("boro_disponible_mg_kg", F.col("boro_disponible_mg_kg").cast(DecimalType(10,2)))
            
            )
        
        return df_n
    

    def filter_rows(self, data:spark.createDataFrame)->spark.createDataFrame:
        
        columns = data.columns
        
        
        for column in columns:
            df = data.filter(~F.col(column).isin("ND")) 

        return df
    

    def clean_data(self, data:spark.createDataFrame)->spark.createDataFrame:

        df = (
            data
            .dropDuplicates()
            .dropna()
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
