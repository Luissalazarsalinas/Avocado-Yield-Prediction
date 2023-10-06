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
            list_folders = [
                self.full_yield_path, self.full_env_path,
                self.full_soil_path , self.full_clean_path,self.full_curate_path
            ]

            # loop to check if the status of the folders
            for file in list_folders:
                folder_exist = self.hdfs_client.status(file, strict=False)
                if folder_exist is not None:
                    # Delete folder and files
                    self.hdfs_client.delete(file, recursive=True)

            # make folders
            for folder in list_folders:
                self.hdfs_client.makedirs(folder)
            
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