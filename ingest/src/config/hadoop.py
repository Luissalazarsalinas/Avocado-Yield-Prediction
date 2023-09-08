from hdfs import InsecureClient
#from config.config import *

class Hadoop:

    def __init__(self, url:str, user:str):

        self.__url = url
        self.__user = user
    
    def __connection(self):

        while True:

            try:

                # Connect to hdfs with insecureclient[secure mode off]
                self.hdfs_client = InsecureClient(url=self.__url, user=self.__user)
                print("Success Connection")
                break
            except Exception as e:
                print("Fail Connection : {e}")
                break
                
    def make_folder(self, path:str):

        # Connection to HDFS
        self.__connection()

        # full path
        self.full_path = path + "/crops"

        try:

            # Check if the folders already exist
            folder_exist = self.hdfs_client.status(self.full_path, strict=False)

            if folder_exist is not None:
                # Delete folder and files
                self.hdfs_client.delete(folder_exist, recursive=True)

            else:
                #Create folder
                self.hdfs_client.makedirs(folder_exist)

        except Exception as e:
            print(f"Fail the creation of folders: {e}")

    def upload_data(self, tmp_folder: str):

        try:
            self.hdfs_client.upload(self.full_path, tmp_folder)
        except Exception as e:
            print(f"Upload of files failed : {e}")