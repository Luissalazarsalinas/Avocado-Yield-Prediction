from datetime import date
from processing.config.sparkconfig import spark
from processing.utils.soil_functions import SoilClean
from processing.utils.schemas import schema_s

def clean_soil_data():
    ## read data and create a dataframe 
    path = "hdfs://localhost:9000/user/User/raw/Colombia/crops/soil/" + str(date.today()) + "/Resultados_de_An_lisis_de_Laboratorio_Suelos_en_Colombia.csv" 
    df = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("multiline", "true")
        .schema(schema_s)
        .csv(path)
        .cache()
    )

    # create instance of class
    soil_clean = SoilClean()

    # modify columns
    df_m = soil_clean.modify_columns(df)
    df_d = soil_clean.drop_columns(df_m)
    # filter and drop rows
    df_r = soil_clean.replace_values(df_d)
    df_f = soil_clean.filter_rows(df_r)

    # cleaning the data
    df_c = soil_clean.clean_data(df_f)

    # save the data in parquet format
    path_out = "hdfs://localhost:9000/user/User/clean/Colombia/crops/" + str(date.today()) + "/soil_clean.parquet" 
    soil_clean.save(df_c,path_out)

# if __name__ == "__main__":
    
#     clean_soil_data()