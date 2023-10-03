import datetime
from hdfs import InsecureClient
#from config.config import settings

class Hadoop:

    def __init__(self, url:str="http://localhost:9870", user:str="User"):

        self.__url = url
        self.__user = user
    
    def __connection(self):
         
         try:
                # Connect to hdfs with insecureclient[secure mode off]
                self.hdfs_client = InsecureClient(url=self.__url, user=self.__user)
        
         except Exception as e:
             print(f"Fail Connection : {e}")
            
                
    def make_folder(self, path:str, 
                    path_evn:str, path_soil:str,
                    path_clean:str, path_curate:str):

        # Connection to HDFS
        self.__connection()

        # full paths
        self.full_yield_path = path + str(datetime.date.today())
        self.full_env_path = path_evn + str(datetime.date.today())
        self.full_soil_path = path_soil + str(datetime.date.today())
        self.full_clean_path = path_clean + str(datetime.date.today())
        self.full_curate_path = path_curate 

        try:

            # Check if the folders already exist
            folder_yield_exist = self.hdfs_client.status(self.full_yield_path, strict=False)
            folder_env_exist = self.hdfs_client.status(self.full_env_path, strict=False)
            folder_soil_exist = self.hdfs_client.status(self.full_soil_path, strict=False)
            folder_clean_exist = self.hdfs_client.status(self.full_clean_path, strict=False)
            folder_curate_exist = self.hdfs_client.status(self.full_curate_path, strict=False)

            if folder_yield_exist is not None:
                # Delete folder and files
                self.hdfs_client.delete(self.full_yield_path, recursive=True)
            elif folder_env_exist is not None:
                self.hdfs_client.delete(self.full_env_path, recursive=True)
            elif folder_soil_exist is not None:
                self.hdfs_client.delete(self.full_soil_path, recursive=True)
            elif folder_clean_exist is not None:
                self.hdfs_client.delete(self.full_clean_path, recursive=True)
            elif folder_curate_exist is not None:
                self.hdfs_client.delete(self.full_curate_path, recursive=True)
            else:
                #Create folder
                self.hdfs_client.makedirs(self.full_yield_path)
                self.hdfs_client.makedirs(self.full_env_path)
                self.hdfs_client.makedirs(self.full_soil_path)
                self.hdfs_client.makedirs(self.full_clean_path)
                self.hdfs_client.makedirs(self.full_curate_path)

        except Exception as e:
            print(f"Fail folders creation: {e}")

    def upload_data(self, path:str, tmp_folder: str):

        try:
            if "yield" in path:
                self.hdfs_client.upload(self.full_yield_path, tmp_folder)
            elif "env" in path:
                self.hdfs_client.upload(self.full_env_path, tmp_folder)
            elif "soil" in path:
                self.hdfs_client.upload(self.full_soil_path, tmp_folder)

        except Exception as e:
            print(f"Upload of files failed : {e}")