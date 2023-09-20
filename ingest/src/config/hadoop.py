import datetime
from hdfs import InsecureClient
from config.config import settings

class Hadoop:

    def __init__(self, url:str=settings.url, user:str=settings.user):

        self.__url = url
        self.__user = user
    
    def __connection(self):
         
         try:
                # Connect to hdfs with insecureclient[secure mode off]
                self.hdfs_client = InsecureClient(url=self.__url, user=self.__user)
        
         except Exception as e:
             print(f"Fail Connection : {e}")
            
                
    def make_folder(self, path:str, path_evn:str, path_soil:str):

        # Connection to HDFS
        self.__connection()

        # full paths
        self.full_yield_path = path + str(datetime.date.today())
        self.full_env_path = path_evn + str(datetime.date.today())
        self.full_soil_path = path_soil + str(datetime.date.today())

        try:

            # Check if the folders already exist
            folder_yield_exist = self.hdfs_client.status(self.full_yield_path, strict=False)
            folder_env_exist = self.hdfs_client.status(self.full_env_path, strict=False)
            folder_soil_exist = self.hdfs_client.status(self.full_soil_path, strict=False)

            if folder_yield_exist is not None:
                # Delete folder and files
                self.hdfs_client.delete(folder_yield_exist, recursive=True)
            elif folder_env_exist is not None:
                self.hdfs_client.delete(folder_env_exist, recursive=True)
            elif folder_soil_exist is not None:
                self.hdfs_client.delete(folder_soil_exist, recursive=True)
            else:
                #Create folder
                self.hdfs_client.makedirs(folder_yield_exist)
                self.hdfs_client.makedirs(folder_env_exist)
                self.hdfs_client.makedirs(folder_soil_exist)

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