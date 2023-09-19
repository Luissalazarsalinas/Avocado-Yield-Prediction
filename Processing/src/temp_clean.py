from config.sparkconfig import spark
from utils.temp_functions import TempClean
from utils.schemas import schema_p_t

# path of files
path = ""
path1 = ""
path2 = ""

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
path_out = ""
temp_clean.save(df_a, path_out)