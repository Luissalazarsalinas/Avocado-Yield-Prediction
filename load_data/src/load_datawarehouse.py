from Config.config import settings
from processing.src.config.sparkconfig import spark

## Read delta lake tables 

df = (
    spark.read
    .format("delta")
    .load("add path from curate zone/cultivo='agucate'")
)

post_url = f"jdbc:postgresql://{settings}:{settings}/{settings}"

properties = {
    "user":f"{settings}",
    "password":f"{settings}",
    "driver":"org.postgresql.Driver"
}

(
    df.write
    .jdbc(
        url= post_url,
        table= "standing_crops",
        mode= "overwrite",
        properties= properties
    )
)
