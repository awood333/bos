from azure.storage.blob import BlobServiceClient as bsc
import pandas as pd
# 
connection_string   = "DefaultEndpointsProtocol=https;AccountName=cs110032001a7ce5ce6;AccountKey=PcDl7DoZRGM7GgBcorDxWsdPm2cL16jL5SljkyN+K5tfvYRlPFrjZ7VClhprUW1uXal3kBirYLQI+AStyYhUjQ==;EndpointSuffix=core.windows.net"
container_name      = 'alanw'
blob_name           = 'alanw'

blob_service_client = bsc.from_connection_string(connection_string)
# connection_client   = bsc.get_container_client(container_name)
container_client    = blob_service_client.get_container_client(container_name)
# 
blobs = container_client.list_blobs()
for blob in blobs:
    print(blob.name)