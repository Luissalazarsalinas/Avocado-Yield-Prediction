import time
import wget
from config.hadoop import Hadoop
from src.utils import urls


# Output path
out_path = "D:/Data_projects/Avocado_Project/Project/Avocado-Yield-Prediction/ingest/src/tmp"

# download data
for url in urls:
    data = wget.download(url=url, out= out_path)
    time.sleep(20) # wait 20 seconds before download the next file

############################ CREATE FOLDERS AND UPLOAD DATA INTO HDFS  #####################################

# Instance
hadoop = Hadoop()

# Create folder
row_path = "row/Colombia"
##
hadoop.make_folder(path=row_path)

# files name
avocado_data = "/Evaluaciones_Agropecuarias_Municipales_EVA.csv"
temp_data = "/Datos_Hidrometeorol_gicos_Crudos_-_Red_de_Estaciones_IDEAM___Temperatura.csv"
soil_data = "/Resultados_de_An_lisis_de_Laboratorio_Suelos_en_Colombia.csv"
url_precip = "/Precipitaci_n.csv"
url_precip1 = "/Precipitaci_n (1).csv"
url_precip2 = "/Precipitaci_n (2).csv"
url_precip3 = "/Precipitaci_n (3).csv"
url_precip4 = "/Precipitaci_n (4).csv"
url_precip5 = "/Precipitaci_n (5).csv"
url_precip6 = "/Precipitaci_n (6).csv"

#List of files
list_files = [avocado_data, temp_data, soil_data, 
              url_precip, url_precip2, url_precip3,
              url_precip4, url_precip5, url_precip6]

for file in list_files:
    out_path_file = out_path + file
    hadoop.upload_data(out_path_file)
    time.sleep(30) # wait 30 second before upload the next file