
account_name = 'cs110032001a7ce5ce6'
container_name = 'alanw'
blob_name = 'stop1.csv'
file_name = 'stop1.csv'
destination_path = 'F:\\COWS\\data\\testdata\\' + blob_name
destination_dir = 'F:\\COWS\\data\\testdata\\'

import os
from datetime import datetime, timedelta
from azure.storage.blob import generate_account_sas, ResourceTypes, AccountSasPermissions, BlobServiceClient, ContainerClient

def generate_sas_token(account_name, account_key, container_name, blob_name):
    # ... (your existing code)

    sas_token_expiry = datetime.utcnow() + timedelta(days=1)

    sas_token = generate_account_sas(
        account_name=account_name,
        account_key=account_key,
        resource_types=ResourceTypes(object=True),
        permission=AccountSasPermissions(read=True, list=True),
        expiry=sas_token_expiry,
        start=datetime.utcnow() - timedelta(minutes=5),
    )

    return sas_token

def download_blob_from_azure(account_name, container_name, file_name, destination_dir, sas_token):
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=sas_token)

        # Get the container
        container_client = blob_service_client.get_container_client(container_name)

        # ... (your existing code)

    except Exception as e:
        print(f"Error accessing Azure Blob Storage: {e}")

if __name__ == "__main__":
    account_name = 'cs110032001a7ce5ce6'
    account_key = os.environ.get('ACCOUNT_KEY')
    container_name = 'alanw'
    file_name = 'stop1.csv'
    destination_dir = 'F:\\COWS\\data\\testdata\\'

    # Get the SAS token
    SAS_TOKEN = generate_sas_token(account_name, account_key, container_name, file_name)

    download_blob_from_azure(account_name, container_name, file_name, destination_dir, SAS_TOKEN)


# import os
# from datetime import datetime, timedelta
# from azure.storage.blob import generate_account_sas, ResourceTypes, AccountSasPermissions, BlobServiceClient, ContainerClient

# def generate_sas_token(account_name, account_key, container_name, blob_name):
#     # ... (your existing code)

#     sas_token_expiry = datetime.utcnow() + timedelta(days=1)

#     sas_token = generate_account_sas(
#         account_name=account_name,
#         account_key=account_key,
#         resource_types=ResourceTypes(object=True),
#         permission=AccountSasPermissions(read=True, list=True),
#         expiry=sas_token_expiry,
#         start=datetime.utcnow() - timedelta(minutes=5),
#     )

#     return sas_token

# def download_blob_from_azure(account_name, container_name, file_name, destination_dir, sas_token):
#     try:
#         # Create a BlobServiceClient
#         blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=sas_token)

#         # Get the container
#         container_client = blob_service_client.get_container_client(container_name)

#         # ... (your existing code)

#     except Exception as e:
#         print(f"Error accessing Azure Blob Storage: {e}")

# if __name__ == "__main__":
#     account_name = 'cs110032001a7ce5ce6'
#     account_key = os.environ.get('ACCOUNT_KEY')
#     container_name = 'alanw'
#     file_name = 'stop1.csv'
#     destination_dir = 'F:\\COWS\\data\\testdata\\'

#     # Get the SAS token
#     SAS_TOKEN = generate_sas_token(account_name, account_key, container_name, file_name)

#     download_blob_from_azure(account_name, container_name, file_name, destination_dir, SAS_TOKEN)
