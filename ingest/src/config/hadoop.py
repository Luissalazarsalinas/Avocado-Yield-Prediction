from hdfs import InsecureClient

# Connect to hdfs 
hdfs_client = InsecureClient("localhost:9000", user="xxxx")

# folders path 
raw_path = "/raw/colombia/crops"

## Create folders 
def create_folders():
    
    try:
        
        # Check if the folders already exist
        folder_exist = hdfs_client.status(raw_path, strict=False)

        if folder_exist is not None:
            hdfs_client.delete(folder_exist, recursive=True)

        else:
            hdfs_client.makedirs(folder_exist) #Create folders

    except Exception as e:
        print(f"Error: {e}")

# Upload data into the folder
def upload_data(tmp_folder: str):

    try:
        # upload data
        hdfs_client.upload(raw_path, tmp_folder)
    except Exception as e:
        print(f"Error: {e}")
