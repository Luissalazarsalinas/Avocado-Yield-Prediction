import pyspark.sql.functions as F
from config.sparkconfig import spark


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
    df2 = df1.join(soil_df.select(F.col("Topografia"), 
                                  F.col("Drenaje"),
                                  F.col("ph_agua_suelo_2_5_1_0"),
                                  F.col("materia_org_nica_mo_porcent"),
                                  F.col("f_sforo_p_bray_ii_mg_kg"),
                                  F.col("azufre_s_fosfato_monocalcico_mg_kg"),
                                  F.col("calcio_ca_intercambiable_cmol_kg"),
                                  F.col("magnesio_mg_intercambiable_cmol_kg"),
                                  F.col("potasio_k_intercambiable_cmol_kg"),
                                  F.col("sodio_na_intercambiable_cmol_kg"),
                                  F.col("capacidad_de_intercambio_cationico_cice_suma_de_bases_cmol_kg"),
                                  F.col("conductividad_el_ctrica_ce_relacion_2_5_1_0_ds_m"),
                                  F.col("hierro_fe_disponible_olsen_mg_kg"),
                                  F.col("cobre_cu_disponible_mg_kg"),
                                  F.col("manganeso_mn_disponible_olsen_mg_kg"),
                                  F.col("zinc_zn_disponible_olsen_mg_kg"),
                                  F.col("boro_b_disponible_mg_kg")), "Municipio", "inner")
    return df2

def save(df:spark.createDataFrame, path:str)->spark.createDataFrame:

    (
        df.write
        .format("parquet")
        .partitionBy(
            "cultivo"
        )
        .mode("overwrite")
        .save(path)
    )
