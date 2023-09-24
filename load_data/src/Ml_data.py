from pathlib import Path
from processing.src.config.sparkconfig import spark


def ml_data():
    # path 
    BASE_PATH = Path(__file__).resolve(strict=True).parent.as_posix()

    ## Read delta lake tables 
    df = (
        spark.read
        .format("delta")
        .load("add path from curate zone/cultivo='agucate'")
    )


    # save data 

    (
        df.write
        .mode("overwrite")
        .parquet(f"{BASE_PATH}/ml_data/ml_data.parquet")
    
    )
