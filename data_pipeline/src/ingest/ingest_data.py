import time
import wget
from ingest.config.hadoop import Hadoop
from ingest.Utils.utils import urls
from ingest.Utils.utils import avocado_data,soil_data,list_temp,list_prec
from pathlib import Path


def ingest():
    # Output path
    BASE_PATH = Path(__file__).resolve(strict=True).parent.as_posix()
    out_path = f"{BASE_PATH}/tmp"

    # download data
    for url in urls:
        data = wget.download(url=url, out= out_path)
        time.sleep(20) # wait 20 seconds before download the next file

    ############################ CREATE FOLDERS AND UPLOAD DATA INTO HDFS  #####################################

    # Instance
    hadoop = Hadoop()

    # Create folder
    ## Raw zone 
    raw_yield_path = "raw/Colombia/crops/yield/"
    raw_env_path = "raw/Colombia/crops/env/"
    raw_soil_path = "raw/Colombia/crops/soil/"
    # Clean zone 
    clean_path = "clean/Colombia/crops/"
    # Curate zone 
    curate_path = "curate/Colombia/crops/"

    ##
    hadoop.make_folder(path=raw_yield_path, 
                    path_evn=raw_env_path,
                    path_soil=raw_soil_path,
                    path_clean= clean_path,
                    path_curate= curate_path)

    # upload avocado yield data
    av_out_path_f = out_path + str(avocado_data)
    hadoop.upload_data(raw_yield_path, av_out_path_f)

    # upload soil data
    s_out_path_f = out_path + str(soil_data)
    hadoop.upload_data(raw_soil_path, s_out_path_f)

    # upload temp data
    for file in list_temp:
        out_path_file = out_path + file
        hadoop.upload_data(raw_env_path,out_path_file)
        time.sleep(30) # wait 30 second before upload the next file

    # upload temp data
    for file in list_prec:
        out_path_file_pre = out_path + file
        hadoop.upload_data(raw_env_path,out_path_file_pre)
        time.sleep(30) # wait 30 second before upload the next file


## call fuction
if __name__=="__main__":

    ingest()