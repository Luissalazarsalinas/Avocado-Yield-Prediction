
from config.sparkconfig import spark
from utils.avocado_functions import AvocadoClean
from utils.schemas import schema_avocado

## read data and create a dataframe 
path = ""
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
path_out = ""
avocado_clean.save(df_c,path_out)