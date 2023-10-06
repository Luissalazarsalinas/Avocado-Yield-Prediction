from pathlib import Path
from processing.src.config.sparkconfig import spark


def ml_data():
    # path 
    BASE_PATH = Path(__file__).resolve(strict=True).parent.as_posix()

    ## Read delta lake tables 
    ## get only the data for avocado crop
    df = (
        spark.read
        .format("delta")
        .load("hdfs://localhost:9000/curate/Colombia/crops/cultivo='agucate'")
    )


    # save data 

    (
        df.write
        .mode("overwrite")
        .parquet(f"{BASE_PATH}/ml_data/ml_data.parquet")
    
    )
