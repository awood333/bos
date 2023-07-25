
import os
from datetime import datetime, timedelta
from azure.storage.blob import generate_account_sas, ResourceTypes, AccountSasPermissions, BlobServiceClient

account_name = 'cs110032001a7ce5ce6'
container_name = 'alanw'
blob_name = 'stop1.csv'
file_name = 'stop1.csv'
destination_path = 'F:\\COWS\\data\\testdata\\' + blob_name
destination_dir = 'F:\\COWS\\data\\testdata\\'

def generate_sas_token(account_name, account_key, container_name, blob_name):
    sas_token_expiry = datetime.utcnow() + timedelta(days=1)
    sas_token = generate_account_sas(
        account_name=account_name,
        account_key=account_key,
        resource_types=ResourceTypes(object=True),
        permission=AccountSasPermissions(read=True, write=True, delete=True, list=True, add=True, create=True, update=True),
        expiry=sas_token_expiry,
        start=datetime.utcnow() - timedelta(minutes=5),
    )

    return sas_token

def download_blob_from_azure(account_name, container_name, file_name, destination_dir, sas_token):
    try:
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=sas_token)
        container_client = blob_service_client.get_container_client(container_name)

        # Download the blob to the specified destination path
        blob_client = container_client.get_blob_client(blob=blob_name)
        with open(os.path.join(destination_dir, file_name), "wb") as f:
            data = blob_client.download_blob()
            data.readinto(f)

    except Exception as e:
        print(f"Error accessing Azure Blob Storage: {e}")

if __name__ == "__main__":
    account_key = os.environ.get('account_key')
    
    # Get the SAS token
    SAS_TOKEN = generate_sas_token(account_name, account_key, container_name, file_name)
    download_blob_from_azure(account_name, container_name, file_name, destination_dir, SAS_TOKEN)
