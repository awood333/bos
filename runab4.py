import subprocess
import os

# Define the list of blob filenames you want to download
blob_filenames = ['stop1.csv', 'bd.csv', 'lbpivot.csv']

# Get the account key from the environment variable
account_key = os.environ.get('account_key')

# Call the ab4.py script for each blob filename in the list
for blob_name in blob_filenames:
    subprocess.run(["python", "azure_blob.py", blob_name, account_key])
