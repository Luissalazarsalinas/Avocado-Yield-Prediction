import os
import time
import wget
from config.hadoop import create_folders, upload_data

# urls
url_avocado = ""
url_temperature = ""
url_soil = ""
url_precip1 = ""
url_precip2 = ""
url_precip3 = ""
# out path
out_path = "F:/Data_projects/Avocado_Project/Project/Avocado-Yield-Prediction/ingest/src/tmp"
# Create a list of urls
url_list = [url_avocado, url_temperature, url_soil, url_precip1, url_precip2, url_precip3]

# download data
for url in url_list:
    data = wget.download(url=url, out= out_path)
    time.sleep(60) # wait a minute before download the next file

# Create the folders
create_folders

# upload data into hdfs
avocado_data = "/"
temp_data = "/"
soil_data = "/"
url_precip1 = "/"
url_precip2 = "/"
url_precip3 = "/"
#List of files
list_files = [avocado_data, temp_data, soil_data, url_precip1, url_precip2, url_precip3]

for file in list_files:
    out_path_file = out_path + file
    upload_data(out_path_file)
    time.sleep(30) # wait 30 second before upload the next file