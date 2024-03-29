from datetime import date
from processing.config.sparkconfig import spark
from processing.utils.temp_functions import TempClean
from processing.utils.schemas import schema_p_t


def clean_temp_data():
    # path of files
    path = "hdfs://localhost:9000/user/User/raw/Colombia/crops/env/" + str(date.today()) + "/Datos_Hidrometeorol_gicos_Crudos_-_Red_de_Estaciones_IDEAM___Temperatura.csv"
    path1 = "hdfs://localhost:9000/user/User/raw/Colombia/crops/env/" + str(date.today()) + "/Datos_Hidrometeorol_gicos_Crudos_-_Red_de_Estaciones_IDEAM___Temperatura (1).csv"
    path2 = "hdfs://localhost:9000/user/User/raw/Colombia/crops/env/" + str(date.today()) + "/Datos_Hidrometeorol_gicos_Crudos_-_Red_de_Estaciones_IDEAM___Temperatura (2).csv"

    # read data
    df = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("multiline", "true")
        .schema(schema_p_t)
        .csv(path)
        .cache()
    )

    df1 = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("multiline", "true")
        .schema(schema_p_t)
        .csv(path1)
        .cache()
    )

    df2 = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("multiline", "true")
        .schema(schema_p_t)
        .csv(path2)
        .cache()
    )


    ## Intance
    temp_clean = TempClean()

    ### modify columns
    df_m = temp_clean.modify_columns(df)
    df_m1 = temp_clean.modify_columns(df1)
    df_m2 = temp_clean.modify_columns(df2)

    # cleaning data
    df_c = temp_clean.clean_data_temp(df_m)
    df_c1 = temp_clean.clean_data_temp(df_m1)
    df_c2 = temp_clean.clean_data_temp(df_m2)

    # union the dataframes
    df_u = temp_clean.union(df_c, df_c1, df_c2)

    ## compute the annual temp by departamento and municipio
    df_a = temp_clean.aggregation(df_u)

    ## save in a parquet format
    path_out = "hdfs://localhost:9000/user/User/clean/Colombia/crops/" + str(date.today()) + "/temp_clean.parquet"
    temp_clean.save(df_a, path_out)


# if __name__ == "__main__":
    
#     clean_temp_data()