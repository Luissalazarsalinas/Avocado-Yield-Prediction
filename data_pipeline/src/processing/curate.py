from datetime import date
from config.sparkconfig import spark
from utils.trans_functions import join_df, save


def cuarate_data():
    
    # paths 
    path_av = "hdfs://localhost:9000/user/User/clean/Colombia/crops/" + "2023-10-07" + "/avocado_clean.parquet"
    path_tp = "hdfs://localhost:9000/user/User/clean/Colombia/crops/" + "2023-10-07" + "/temp_clean.parquet"
    path_prec = "hdfs://localhost:9000/user/User/clean/Colombia/crops/"+ "2023-10-07" + "/prec_clean.parquet"
    path_soil = "hdfs://localhost:9000/user/User/clean/Colombia/crops/" + "2023-10-07" + "/soil_clean.parquet"

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
    path_out = "hdfs://localhost:9000/user/User/curate/Colombia/crops/curate_data.parquet"
    save(df_j, path_out)

if __name__ == "__main__":

    cuarate_data()

