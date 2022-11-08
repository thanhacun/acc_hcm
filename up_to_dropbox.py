# REF: https://practicaldatascience.co.uk/data-science/how-to-use-the-dropbox-api-with-python
# REF: https://stackoverflow.com/a/71794390

import pathlib
import pandas as pd
import dropbox
from dropbox.exceptions import AuthError
from dotenv import dotenv_values

keys = dotenv_values('./.env_dropbox')
APP_KEY = keys['APP_KEY']
APP_SECRET = keys['APP_SECRET']
REFRESH_TOKEN = keys['REFRESH_TOKEN']

def dropbox_connect():
    """Create a connection to Dropbox."""

    try:
        # dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
        dbx = dropbox.Dropbox(app_key=APP_KEY, app_secret=APP_SECRET, oauth2_refresh_token=REFRESH_TOKEN)
    except AuthError as e:
        print('Error connecting to Dropbox with access token: ' + str(e))
    return dbx

def dropbox_list_files(path):
    """Return a Pandas dataframe of files in a given Dropbox folder path in the Apps directory.
    """

    dbx = dropbox_connect()

    try:
        files = dbx.files_list_folder(path).entries
        files_list = []
        for file in files:
            if isinstance(file, dropbox.files.FileMetadata):
                revisions = dbx.files_list_revisions(path + '/' + file.name, limit=31).entries
                metadata = {
                    'id': file.id,
                    'name': file.name,
                    'path_display': file.path_display,
                    'client_modified': file.client_modified,
                    'server_modified': file.server_modified,
                    'hash': file.content_hash,
                    'size': file.size,
                    'revisions': [{
                         'id': r.id,
                         'name': r.name,
                         'hash': r.content_hash,
                     } for r in revisions]
                }
                metadata['hashs'] = [r['hash'] for r in metadata['revisions']] + [metadata['hash']]
                files_list.append(metadata)

        df = pd.DataFrame.from_records(files_list)
        return df.sort_values(by='server_modified', ascending=False)

    except Exception as e:
        print('Error getting list of files from Dropbox: ' + str(e))

def dropbox_download_file(dropbox_file_path, local_file_path):
    """Download a file from Dropbox to the local machine."""

    try:
        dbx = dropbox_connect()

        with open(local_file_path, 'wb') as f:
            metadata, result = dbx.files_download(path=dropbox_file_path)
            f.write(result.content)
    except Exception as e:
        print('Error downloading file from Dropbox: ' + str(e))
        if isinstance(e, dropbox.exceptions.ApiError):
            print(type(e.error))
        raise e

def dropbox_upload_file(local_path, local_file, dropbox_file_path):
    """Upload a file from the local machine to a path in the Dropbox app directory.

    Args:
        local_path (str): The path to the local file.
        local_file (str): The name of the local file.
        dropbox_file_path (str): The path to the file in the Dropbox app directory.

    Example:
        dropbox_upload_file('.', 'test.csv', '/stuff/test.csv')

    Returns:
        meta: The Dropbox file metadata.
    """

    try:
        dbx = dropbox_connect()

        local_file_path = pathlib.Path(local_path) / local_file

        with local_file_path.open("rb") as f:
            meta = dbx.files_upload(f.read(), dropbox_file_path, mode=dropbox.files.WriteMode("overwrite"))

            return meta
    except Exception as e:
        print('Error uploading file to Dropbox: ' + str(e))

