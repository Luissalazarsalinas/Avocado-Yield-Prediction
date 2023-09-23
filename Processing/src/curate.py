from config.sparkconfig import spark
from utils.trans_functions import join_df, save


# paths 
path_av = ""
path_tp = ""
path_prec = ""
path_soil = ""

# read data
df_av = (
    spark.read
    .parquet(path_av)
)

df_tp = (
    spark.read
    .parquet(path_tp)
)

df_prec = (
    spark.read
    .parquet(path_prec)
)

df_soil = (
    spark.read
    .parquet(path_soil)
)

## join dataframes
df_j = join_df(
    df_av,
    df_tp,
    df_prec,
    df_soil
)

# partititons and save data in delta format
path_out = ""
save(df_j, path_out)

spark.stop()
