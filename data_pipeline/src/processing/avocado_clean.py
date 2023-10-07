
from datetime import date
from config.sparkconfig import spark
from utils.avocado_functions import AvocadoClean
from utils.schemas import schema_avocado

## read data and create a dataframe 
# def clean_avacado_data():

path = "hdfs://localhost:9000/user/User/raw/Colombia/crops/yield/" + str(date.today()) + "/Evaluaciones_Agropecuarias_Municipales_EVA.csv"

df = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("multiline", "true")
    .schema(schema_avocado)
    .csv(path)
    .cache()
)

# create instance of class
avocado_clean = AvocadoClean()

# modify columns
df_m = avocado_clean.modify_column(df)

# cleaning the data
df_c = avocado_clean.cleaning(df_m)

# save the data in parquet format
path_out = "hdfs://localhost:9000/user/User/clean/Colombia/crops/" + str(date.today()) + "/avocado_clean.parquet"
avocado_clean.save(df_c,path_out)

# if __name__ == "__main__":
    
#     clean_avacado_data()