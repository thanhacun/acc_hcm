import os, argparse, dropbox, time, re
from os import walk
from math import ceil
# from functools import reduce
# from dropbox.exceptions import AuthError
from dotenv import dotenv_values
import csv
from zipfile import ZipFile, ZIP_DEFLATED

from up_to_dropbox import *
from dropbox_content_hasher import DropboxContentHasher

keys = dotenv_values('./.env_dropbox')
APP_KEY = keys['APP_KEY']
APP_SECRET = keys['APP_SECRET']
REFRESH_TOKEN = keys['REFRESH_TOKEN']

class DropBoxUpload:
    def __init__(self,timeout=900,chunk=8, monthly_mode=False, monthly_regex=''):
        self.APP_KEY = APP_KEY
        self.APP_SECRET = APP_SECRET
        self.REFRESH_TOKEN = REFRESH_TOKEN
        self.timeout = timeout
        self.chunk = chunk
        self.monthly_mode = monthly_mode  # Upload override daily backup file for a month (using dropbox version to restore)
        self.monthly_regex = monthly_regex or '(.*)(\d{8}).*(\..*)'
        self.dbx = dropbox.Dropbox(app_key=self.APP_KEY, app_secret=self.APP_SECRET, oauth2_refresh_token=self.REFRESH_TOKEN)

    def UpLoadFile(self, upload_path, file_path, new_file_path=None):
        # dbx = dropbox.Dropbox(app_key=self.APP_KEY, app_secret=self.APP_SECRET, oauth2_refresh_token=self.REFRESH_TOKEN)
        dbx = self.dbx
        file_size = os.path.getsize(file_path)
        CHUNK_SIZE = self.chunk * 1024 * 1024
        dest_path = upload_path + '/' + os.path.basename(new_file_path or file_path)
        since = time.time()
        meta = None
        with open(file_path, 'rb') as f:
            uploaded_size = 0
            if file_size <= CHUNK_SIZE:
                meta = dbx.files_upload(f.read(), dest_path, mode=dropbox.files.WriteMode("overwrite"))
                time_elapsed = time.time() - since
                print('Uploaded {} {:.2f}%'.format(file_path, 100).ljust(15) + ' --- {:.0f}m {:.0f}s'.format(time_elapsed//60,time_elapsed%60).rjust(15))
            else:
                upload_session_start_result = dbx.files_upload_session_start(f.read(CHUNK_SIZE))
                cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id,
                                                           offset=f.tell())
                commit = dropbox.files.CommitInfo(path=dest_path, mode=dropbox.files.WriteMode("overwrite"))
                while f.tell() <= file_size:
                    if ((file_size - f.tell()) <= CHUNK_SIZE):
                        meta = dbx.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
                        time_elapsed = time.time() - since
                        print('Uploaded {:.2f}%'.format(100).ljust(15) + ' --- {:.0f}m {:.0f}s'.format(time_elapsed//60,time_elapsed%60).rjust(15))
                        break
                    else:
                        dbx.files_upload_session_append_v2(f.read(CHUNK_SIZE),cursor)
                        cursor.offset = f.tell()
                        uploaded_size += CHUNK_SIZE
                        uploaded_percent = 100*uploaded_size/file_size
                        time_elapsed = time.time() - since
                        print('Uploaded {} {:.2f}%'.format(file_path, uploaded_percent).ljust(15) + ' --- {:.0f}m {:.0f}s'.format(time_elapsed//60,time_elapsed%60).rjust(15), end='\r')
        return meta

    def RenameFile(self, remote_folder_path, remote_name, remote_new_name):
        # Delete new_name if exists
        # Rename to new_name

        remote_name_path = f'{remote_folder_path}/{remote_name}'
        remote_new_name_path = f'{remote_folder_path}/{remote_new_name}'
        try:
            rename_meta = self.dbx.files_move(remote_name_path, remote_new_name_path)
        except dropbox.exceptions.ApiError as e:
            if isinstance(e.error, dropbox.files.RelocationError):
                print('File exists => Delete first')
                delete_meta = self.dbx.files_delete(remote_new_name_path)
                if delete_meta.name == remote_new_name:
                    rename_meta = self.dbx.files_move(remote_name_path, remote_new_name_path)
        if rename_meta.name == remote_new_name:
            print('Rename done!')

    def FileHash(self, local_file_path):
        print(f'Compute {local_file_path} content hash -- It may take a long time!')
        hasher = DropboxContentHasher()
        range = 50
        complete_sign = '='
        not_complete_sign = ' '
        file_size = os.path.getsize(local_file_path)
        chunked_size = 0
        percent = int(ceil((chunked_size  / file_size) * 100))
        with open(local_file_path, 'rb') as f:  # Slow for big file
            while True:
                chunk = f.read(1024)
                chunked_size += 1024
                percent = 100 if chunked_size > file_size else int(ceil((chunked_size / file_size) * 100))
                print('[', round(percent * range / 100) * complete_sign, round(range*(100 - percent) / 100) * not_complete_sign, ']', end='\r')
                if len(chunk) == 0:
                    break
                hasher.update(chunk)
        hash_info = hasher.hexdigest()
        return hash_info
    
    def FileNeedUpload(self, remote_folder_path, local_folder_path):
        """ 
            - Return a list of file_path need to uploads
            - NOTE: using hashinfo is slow for big files
            - TODO []: original_name => original_name => delete new_name if exist => rename to new_name
        """
        # - Check /remote_folder_path for history.csv, create if neccessary
        # - Get the not_up_load_file in the local_folder_path by comparing file ids in remote_folder_path with that of in csv file
        # - Return the list of orginal_name and new_name
        file_paths = []
        remote_files_list = dropbox_list_files(remote_folder_path)
        if remote_files_list is not None:
           remote_files_list = remote_files_list.to_dict('records')
        else:
           remote_files_list = []
        local_files_list = next(walk(local_folder_path), [])[2]

        history = None
        try:
            dropbox_download_file(f'{remote_folder_path}/history.csv', './temp_history.csv')
            with open('./temp_history.csv', 'r') as csv_file:
                history = list(csv.DictReader(csv_file))
                # remote_file_ids = [r['id'] for f in remote_files_list for r in f['revisions'] + [{'id': f['id']}]]  # include the file and its revisions
                # remote_file_names = [r['name'] for f in remote_files_list for r in f['revisions'] + [{'name': f['name']}]]
                remote_file_hashs = [r['hash'] for f in remote_files_list for r in (f['revisions'] + [{'hash': f['hash']}])]
                uploaded_original_names = [row['original_name'] for row in history if row['hash'] in set(remote_file_hashs)]
                # print(uploaded_original_names, local_files_list)
                files_need_to_upload = [n for n in local_files_list if n not in uploaded_original_names]
        except dropbox.exceptions.ApiError as e:
            print('Error happen in FileNeedUpload', e)
            files_need_to_upload = [n for n in local_files_list]


        # Method 1: Compare content_hash
        # remote_hashs = list(set(reduce(lambda l1, l2: l1 + l2, [l['hashs'] for l in list(remote_files_list)], [])))
        # local_files_to_upload = []
        # for p in local_files_list:
        #    hasher = DropboxContentHasher()
        #    with open(local_folder_path + '/' + p, 'rb') as f:  # Slow for big file
        #        while True:
        #            chunk = f.read(1024)
        #            if len(chunk) == 0:
        #                break
        #            hasher.update(chunk)
        #    hash_info = hasher.hexdigest()
        #    if hash_info not in remote_hashs:
        #        local_files_to_upload.append(p)
        #    else:
        #        # Already uploaded => SKIP
        #        print(p, ': already uploaded!')

        # Method 2: Compare revision name (using approach up original_name [then delete the new_name] then rename to new_name)
        
        # remote_revision_names = [r['name'] for f in remote_files_list for r in f['revisions']]
        # print(remote_revision_names)
        # for n in local_files_list:
        #     if n in remote_revision_names:
        #         print(n, ': already uploaded!')
        #     else:
        # local_files_to_upload = [n for n in local_files_list if n not in remote_revision_names]
        #         local_files_to_upload.append(n)

        # new_file_paths = []
        new_file_names = []
        if self.monthly_mode:
            new_file_names = map(lambda p: self.MonthlyFileName(p), files_need_to_upload)

        file_paths = list(map(lambda p: f'{local_folder_path}/{p}', files_need_to_upload))
        # new_file_paths = list(map(lambda p: remote_folder_path + '/' + p, new_file_paths or file_paths))

        result = list((pair for pair in zip(file_paths, new_file_names or files_need_to_upload)))
        return result

    def UpdateHistory(self, remote_folder_path, values):
        """
            - values: list of dict(id, original_name, new_name)
        """
        # - Write to the csv file values
        rows = []
        try:
            with open('./temp_history.csv', 'r') as csv_file:
                rows = list(csv.DictReader(csv_file))
            with open(f'./temp_history.csv', 'w') as csv_file:
                field_names = ['id', 'original_name', 'new_name', 'hash', 'server_modified']
                writer = csv.DictWriter(csv_file, fieldnames=field_names)
                writer.writeheader()
                writer.writerows(rows)
                for row in values:
                    writer.writerow(row)
            meta = self.UpLoadFile(remote_folder_path, './temp_history.csv', 'history.csv')
            return meta
        except Exception as e:
            print('Error happen in UpdateHistory', e)
            return False

    def MonthlyFileName(self, original_filename):
        """ Return the file name for monthly mode based on the regex pattern"""
        if self.monthly_mode:
            file_name = ''
            for s in re.split(self.monthly_regex, original_filename):
                if s:
                    if re.match('\d{8}', s):
                        file_name += s[:6]
                    else:
                        file_name += s
        else:
            file_name = original_filename
        return file_name

    def ZipFile(self, local_file_path, local_zip_path):
        print(f'Zipping {local_file_path} ... -- It may take time!')
        try:
            with ZipFile(local_zip_path, 'w', ZIP_DEFLATED, compresslevel=5) as zip_file:
                zip_file.write(local_file_path)
        except Exception as e:
            print('Error happen in ZipFile', e)
            raise e

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('upload_path', type=str, help='path in dropbox, empty if root folder')
    parser.add_argument('file_path', type=str, help='path to file to upload, in monthly_mode this is the folder name')
    parser.add_argument('--mode', type=str, default='', help='Upload mode: default, folder or monthly')
    parser.add_argument('--re', type=str, default='', help='regex for daily backup file to extract filename and datetime')  # only need in customize case
    parser.add_argument('--zip', type=bool, default=False, help='Zip the file or not')
    parser.add_argument('--timeout', type=int, default=900)
    parser.add_argument('--chunk', type=int, default=8, help='chunk size in MB')
    args = parser.parse_args()

    if args.mode in  ['folder', 'monthly']:
        dbu = DropBoxUpload(timeout=args.timeout, chunk=args.chunk, monthly_mode=True if args.mode == 'monthly' else False)
        file_paths = dbu.FileNeedUpload(args.upload_path, args.file_path)
        update_history_rows = []
        for path, new_name in file_paths:
            print(path, '=>', path, 'on Dropbox', '=>', new_name)
            if args.zip:
                try:
                    dbu.ZipFile(path, './temp.zip')
                    meta = dbu.UpLoadFile(args.upload_path, './temp.zip', path.split('/')[-1] + '.zip')
                except Exception as e:
                    print('Error happen', e)
            else:
                meta = dbu.UpLoadFile(args.upload_path, path, new_name)
            if isinstance(meta, dropbox.files.FileMetadata):
                update_history_rows.append({
                    'id': meta.id, 'original_name': path.split('/')[-1], 
                    'new_name': meta.name, 
                    # 'hash': dbu.FileHash('./temp.zip'), 
                    'hase': meta.content_hash,
                    'server_modified': meta.server_modified})
            else:
                print('Upload not successfully.')
        if update_history_rows:
            meta = dbu.UpdateHistory(args.upload_path, update_history_rows)
            if isinstance(meta, dropbox.files.Metadata):
                print('History updated successfully!')

        try:
            os.remove('./temp_history.csv')
            print('Cleanup done!')
        except FileNotFoundError:
            print('File not found', FileNotFoundError)

    else:
        dbu = DropBoxUpload(timeout=args.timeout, chunk=args.chunk)
        meta = None
        if args.zip:
            try:
                dbu.ZipFile(args.file_path, './temp.zip')
                meta = dbu.UpLoadFile(args.upload_path, './temp.zip', args.file_path.split('/')[-1] + '.zip')
                os.remove('./temp.zip')
            except Exception as e:
                print('Error happen', e)
        else:
            meta = dbu.UpLoadFile(args.upload_path, args.file_path)
        if isinstance(meta, dropbox.files.FileMetadata):
            print('Successfully uploaded!')
        else:
            print('Cannot upload!')

if __name__ == "__main__":
    main()
