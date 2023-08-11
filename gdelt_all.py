import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd;
import datetime;
import requests; 
import io;
import zipfile;

try:
    #time range
    start = "20230501000000"
    end= datetime.datetime.now()

    #creating timestamp range
    ts = (pd.DataFrame(columns=['NULL'],
                        index=pd.date_range(start, end,
                                            freq='15T'))
            .index.strftime('%Y%m%d%H%M%S')
            .tolist()
    )                                

    #http://data.gdeltproject.org/gdeltv2/[TIMESTAMP].export.CSV.zip
    #create URL for latest timestamp
    exportUrl = "http://data.gdeltproject.org/gdeltv2/" + ts[-1] + ".export.CSV.zip"
    mentionsUrl = "http://data.gdeltproject.org/gdeltv2/" + ts[-1] + ".mentions.CSV.zip"
    gkgUrl = "http://data.gdeltproject.org/gdeltv2/" + ts[-1] +  ".gkg.csv.zip"
    urls = [ exportUrl, mentionsUrl, gkgUrl]

    #connection string is constant for authentication to storage account
    conn_str = "DefaultEndpointsProtocol=https;AccountName=srckx;AccountKey=sr5U6sb2z/F6QPVU94OXvuM53vu1p16D8CDW8sV28IRzSNWn5hXQyeb5XwUK8NED2wevlhtmSBOC+AStKcKiwA==;EndpointSuffix=core.windows.net"
    
    #loop over each file type, pulled from above
    for u in urls:
        req = requests.get(u)
        ext = zipfile.ZipFile(io.BytesIO(req.content))
        ext.extractall()

        #set container name and create local file 
        if "export" in u:
            local_file= ts[-1] + ".export.CSV"
            container_name = "export"
        elif "mentions" in u:
            local_file= ts[-1] + ".mentions.CSV"
            container_name = "mentions"
        else:
            local_file= ts[-1] + ".gkg.csv"
            container_name = "gkg"    

        #create connection to storage account
        export_blob_client = BlobClient.from_connection_string(conn_str, container_name, blob_name=local_file)   
        
        print("\nUploading to Azure Storage as blob:\n" + local_file)

        # Upload the created file
        with open(file=local_file, mode="rb") as data:
            export_blob_client.upload_blob(data) 

except Exception as ex:
    print('Exception:')
    print(ex)
