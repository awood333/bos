'''
dropbox.py
'''
import dropbox
import csv
from io import BytesIO
from dash import Dash, html

# Replace with your Dropbox access token
DROPBOX_ACCESS_TOKEN = "your_access_token_here"

# Replace with your shared link for the folder
FOLDER_SHARED_LINK = "https://www.dropbox.com/scl/fo/db6f213mn1b5crnr6w4vo/h?dl=0"

# Initialize Dropbox client
dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

# Resolve the shared link to get the direct link to the folder
folder_shared_link_metadata = dbx.sharing_get_shared_link_metadata(url=FOLDER_SHARED_LINK)
folder_direct_link = folder_shared_link_metadata.links[0].url

# List all files in the folder
try:
    folder_metadata = dbx.files_list_folder(folder_direct_link)

    # Loop through each file in the folder
    for file_entry in folder_metadata.entries:
        # Download the file content
        metadata, file_content = dbx.files_download(file_entry.path_display)
        csv_content = file_content.content.decode('utf-8')

        # Process the CSV content
        csv_reader = csv.reader(csv_content.splitlines())

        # Assuming you want to store the data in a list of lists
        csv_data = [row for row in csv_reader]

        # Now csv_data contains all the rows from the CSV file
        # You can access them like csv_data[row_index][column_index]

except dropbox.exceptions.HttpError as err:
    print(f"Error listing or downloading files: {err}")

except Exception as e:
    print(f"An unexpected error occurred: {e}")
