from datetime import date
from config.sparkconfig import spark
from utils.prec_functions import PrecipClean
from utils.schemas import schema_p_t


def clean_precipitation_data():
    # path of files
    path = "hdfs://localhost:9000/raw/Colombia/crops/env/" + str(date.day()) + "/Precipitaci_n.csv"
    path1 = "hdfs://localhost:9000/raw/Colombia/crops/env/" + str(date.day()) + "/Precipitaci_n (1).csv"
    path2 = "hdfs://localhost:9000/raw/Colombia/crops/env/" + str(date.day()) + "/Precipitaci_n (2).csv"
    path3 = "hdfs://localhost:9000/raw/Colombia/crops/env/" + str(date.day()) + "/Precipitaci_n (3).csv"
    path4 = "hdfs://localhost:9000/raw/Colombia/crops/env/" + str(date.day()) + "/Precipitaci_n (4).csv"
    path5 = "hdfs://localhost:9000/raw/Colombia/crops/env/" + str(date.day()) + "/Precipitaci_n (5).csv"
    path6 = "hdfs://localhost:9000/raw/Colombia/crops/env/" + str(date.day()) + "/Precipitaci_n (6).csv"

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

    df3 = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("multiline", "true")
        .schema(schema_p_t)
        .csv(path3)
        .cache()
    )

    df4 = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("multiline", "true")
        .schema(schema_p_t)
        .csv(path4)
        .cache()
    )

    df5 = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("multiline", "true")
        .schema(schema_p_t)
        .csv(path5)
        .cache()
    )

    df6 = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("multiline", "true")
        .schema(schema_p_t)
        .csv(path6)
        .cache()
    )

    ## Intance
    prec_clean = PrecipClean()

    ### modify columns
    df_m = prec_clean.modify_columns(df)
    df_m1 = prec_clean.modify_columns(df1)
    df_m2 = prec_clean.modify_columns(df2)
    df_m3 = prec_clean.modify_columns(df3)
    df_m4 = prec_clean.modify_columns(df4)
    df_m5 = prec_clean.modify_columns(df5)
    df_m6 = prec_clean.modify_columns(df6)

    # cleaning data
    df_c = prec_clean.clean_data(df_m)
    df_c1 = prec_clean.clean_data(df_m1)
    df_c2 = prec_clean.clean_data(df_m2)
    df_c3 = prec_clean.clean_data(df_m3)
    df_c4 = prec_clean.clean_data(df_m4)
    df_c5 = prec_clean.clean_data(df_m5)
    df_c6 = prec_clean.clean_data(df_m6)

    # union the dataframes
    df_u = prec_clean.union(
        df_c, df_c1, df_c2,
        df_c3,df_c4,df5, df_c6
    )

    ## compute the annual temp by departamento and municipio
    df_a = prec_clean.aggregation(df_u)

    ## save in a parquet format
    path_out = "hdfs://localhost:9000/"
    prec_clean.save(df_a, path_out)