import os
import dropbox
from pathlib import Path

class DropboxDownloader:
    def __init__(self):
        # Initialize Dropbox connection
        access_token = os.getenv('dropbox_access_token')
        app_key = os.getenv('dropbox_app_key')
        app_secret = os.getenv('app_secret')
        self.dbx = dropbox.Dropbox(access_token)

    def download_files(self, folder_path='/Apps/wycattle', local_directory='D:/Cows/dropbox/downloads'):
        # Create local directory if it doesn't exist
        Path(local_directory).mkdir(parents=True, exist_ok=True)

        # List files in Dropbox folder
        files = self.list_files(folder_path)

        for file in files:
            # Download each file and save as .csv
            file_content, metadata = self.download_file(file.path_display)
            local_file_path = os.path.join(local_directory, file.name + '.csv')

            with open(local_file_path, 'wb') as local_file:
                local_file.write(file_content)

            print(f"Downloaded: {file.name}.csv")

    def list_files(self, folder_path):
        # List files in Dropbox folder
        result = self.dbx.files_list_folder(folder_path)
        return result.entries

    def download_file(self, file_path):
        # Download file content
        _, file_content = self.dbx.files_download(file_path)
        return file_content.content, file_content.metadata

if __name__ == "__main__":
    # Create an instance of the DropboxDownloader class
    downloader = DropboxDownloader()

    # Specify the folder path in Dropbox and local directory
    folder_path = '/Apps/wycattle'
    local_directory = 'D:/Cows/dropbox/downloads'

    # Download files from Dropbox and save them locally
    downloader.download_files(folder_path, local_directory)
