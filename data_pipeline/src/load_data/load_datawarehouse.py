from Config.config import settings
from processing.src.config.sparkconfig import spark

def load_datawarehouse():
    ## Read delta lake tables
    path = "hdfs://localhost:9000/curate/Colombia/crops/"
    df = (
        spark.read
        .format("delta")
        .load(path)
    )

    post_url = f"jdbc:postgresql://{settings.db_hostname}:{settings.db_port}/{settings.db_name}"

    properties = {
        "user":f"{settings.db_username}",
        "password":f"{settings.db_password}",
        "driver":"org.postgresql.Driver"
    }
    # Save the data into the datawarehouse [standing table]
    (
        df.write
        .jdbc(
            url= post_url,
            table= "standing_crops",
            mode= "overwrite",
            properties= properties
        )
    )
