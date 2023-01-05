import os, argparse, dropbox, time, re
from os import walk
from os.path import basename
from dotenv import dotenv_values
import csv
import types
from functools import partial
from zipfile import ZipFile, ZIP_DEFLATED

from up_to_dropbox import *
from dropbox_content_hasher import DropboxContentHasher

from tqdm import tqdm

keys = dotenv_values('./.env_dropbox')
APP_KEY = keys['APP_KEY']
APP_SECRET = keys['APP_SECRET']
REFRESH_TOKEN = keys['REFRESH_TOKEN']

class DropBoxUpload:
    def __init__(self,timeout=900,chunk=8, monthly_mode=False, monthly_regex='', show_pbar=True):
        self.APP_KEY = APP_KEY
        self.APP_SECRET = APP_SECRET
        self.REFRESH_TOKEN = REFRESH_TOKEN
        self.timeout = timeout
        self.chunk = chunk
        self.monthly_mode = monthly_mode  # Upload override daily backup file for a month (using dropbox version to restore)
        self.monthly_regex = monthly_regex or '(.*)(\d{8}).*(\..*)'
        self.dbx = dropbox.Dropbox(app_key=self.APP_KEY, app_secret=self.APP_SECRET, oauth2_refresh_token=self.REFRESH_TOKEN)
        self.show_pbar = show_pbar

    def UpLoadFile(self, upload_path, file_path, new_file_path=None):
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
                pbar = tqdm(unit='M', unit_scale=True, unit_divisor=1024, total=file_size, disable=not self.show_pbar)
                # pbar.clear()
                upload_session_start_result = dbx.files_upload_session_start(f.read(CHUNK_SIZE))
                cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id,
                                                        offset=f.tell())
                commit = dropbox.files.CommitInfo(path=dest_path, mode=dropbox.files.WriteMode("overwrite"))
                pbar.update(CHUNK_SIZE)
                while f.tell() <= file_size:
                    if ((file_size - f.tell()) <= CHUNK_SIZE):
                        pbar_update = file_size - f.tell()
                        meta = dbx.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
                        pbar.update(pbar_update)
                        # time_elapsed = time.time() - since
                        # print('Uploaded {:.2f}%'.format(100).ljust(15) + ' --- {:.0f}m {:.0f}s'.format(time_elapsed//60,time_elapsed%60).rjust(15))
                        break
                    else:
                        dbx.files_upload_session_append_v2(f.read(CHUNK_SIZE),cursor)
                        cursor.offset = f.tell()
                        uploaded_size += CHUNK_SIZE
                        pbar.update(CHUNK_SIZE)
                        # uploaded_percent = 100*uploaded_size/file_size
                        # time_elapsed = time.time() - since
                        # print('Uploaded {} {:.2f}%'.format(file_path, uploaded_percent).ljust(15) + ' --- {:.0f}m {:.0f}s'.format(time_elapsed//60,time_elapsed%60).rjust(15), end='\r')
                pbar.close()
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
        file_size = os.path.getsize(local_file_path)
        chunked_size = 0
        with open(local_file_path, 'rb') as f:  # Slow for big file
            pbar = tqdm(unit='G', unit_scale=True, unit_divisor=1024, total=file_size, disable=not self.show_pbar)
            pbar.clear()
            while True:
                chunk = f.read(1024)
                chunked_size += 1024
                pbar.update(1024)
                if len(chunk) == 0:
                    break
                hasher.update(chunk)
            pbar.close()
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
            print('Getting uploaded history...')
            dropbox_download_file(f'{remote_folder_path}/history.csv', './temp_history.csv')
            with open('./temp_history.csv', 'r') as csv_file:
                history = list(csv.DictReader(csv_file))
                # remote_file_ids = [r['id'] for f in remote_files_list for r in f['revisions'] + [{'id': f['id']}]]  # include the file and its revisions
                # remote_file_names = [r['name'] for f in remote_files_list for r in f['revisions'] + [{'name': f['name']}]]
                remote_file_hashs = [r['hash'] for f in remote_files_list for r in (f['revisions'] + [{'hash': f['hash']}])]
                uploaded_original_names = [row['original_name'] for row in history if row['hash'] in set(remote_file_hashs)]
                files_need_to_upload = [n for n in local_files_list if n not in uploaded_original_names]
        except dropbox.exceptions.ApiError as e:
            print('Error happen in FileNeedUpload', e)
            files_need_to_upload = [n for n in local_files_list]

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
            print('Update uploaded history...')
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
        """
            - Zipping file with integrated progress bar
            - Inspired: https://stackoverflow.com/questions/28522669/how-to-print-the-percentage-of-zipping-a-file-python/41664456#41664456
            - NOTE: types.MethodType and partial
        """
        def progress(total_size, original_write, self, buf):
            progress.bytes += len(buf)
            progress.obytes += 1024 * 8  # Hardcoded in zipfile.write
            progress.bar.update(1024 * 8)
            return original_write(buf)

        def zipdir(path, ziph):
            # ziph is zipfile handle
            for root, dirs, files in os.walk(path):
                for file in files:
                    ziph.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), os.path.join(path, '..')))

        def dirsize(path):
            res = 0
            for root, dirs, files in os.walk(path):
                for file in files:
                    fp = os.path.join(root, file)
                    res += os.path.getsize(fp)
            return res
    
        total_size = os.path.getsize(local_file_path) if os.path.isfile(local_file_path) else dirsize(local_file_path)
        progress.bar = tqdm(unit='G', unit_scale=True, unit_divisor=1024, total=total_size, disable=not self.show_pbar)
        progress.bar.clear()
        progress.bytes = 0
        progress.obytes = 0
        print(f'Zipping {local_zip_path} ... -- It may take time!')

        if os.path.isfile(local_file_path):
            try:
                with ZipFile(local_zip_path, 'w', ZIP_DEFLATED, compresslevel=5) as _zip:
                    _zip.fp.write = types.MethodType(partial(progress, total_size, _zip.fp.write), _zip.fp)
                    _zip.write(local_file_path, arcname=basename(local_file_path))  # do NOT keep the absolute directory
                progress.bar.close()
            except Exception as e:
                print('Error happen while zipping a file', e)
                raise e
        else:
            try:
                with ZipFile(local_zip_path, 'w', ZIP_DEFLATED, compresslevel=5) as _zip:
                    _zip.fp.write = types.MethodType(partial(progress, total_size, _zip.fp.write), _zip.fp)
                    zipdir(local_file_path, _zip)
                progress.bar.close()
            except Exception as e:
                print('Error happen while zipping a folder', e)
                raise e

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('upload_path', type=str, help='path in dropbox, empty if root folder')
    parser.add_argument('file_path', type=str, help='path to file to upload, in monthly_mode this is the folder name')
    parser.add_argument('--mode', type=str, default='', help='Upload mode: default, folder or monthly')
    parser.add_argument('--re', type=str, default='', help='regex for daily backup file to extract filename and datetime')  # only need in customize case

    # parser.add_argument('--zip', action=argparse.BooleanOptionalAction, help='Zip the file or not')  # Only for python 3.9+
    parser.add_argument('--zip', action='store_true')
    parser.add_argument('--no-zip', dest='zip', action='store_false')
    parser.set_defaults(zip=True)

    parser.add_argument('--timeout', type=int, default=900)
    parser.add_argument('--chunk', type=int, default=8, help='chunk size in MB')

    # parser.add_argument('--pbar', action=argparse.BooleanOptionalAction, help='showing progress bar')  # Only for python 3.9+
    parser.add_argument('--pbar', action='store_true')
    parser.add_argument('--no-pbar', dest='pbar', action='store_false')
    parser.set_defaults(pbar=True)
    
    args = parser.parse_args()

    if args.mode in  ['folder', 'monthly']:
        dbu = DropBoxUpload(timeout=args.timeout, chunk=args.chunk, monthly_mode=True if args.mode == 'monthly' else False, show_pbar=args.pbar)
        # TODO [X]: Handle zip and upload for folder
        if args.mode == 'folder' and args.zip and os.path.isdir(args.file_path):
            dir_name = os.path.basename(args.file_path)
            print(dir_name, '=>', args.upload_path, 'on Dropbox', '=>', dir_name + '.zip')
            try:
                dbu.ZipFile(args.file_path, './temp.zip')
                meta = dbu.UpLoadFile(args.upload_path, './temp.zip', dir_name +'.zip')
                if isinstance(meta, dropbox.files.FileMetadata):
                    os.remove('./temp.zip')
                return meta
            except Exception as e:
                print(f'Error while zipping and sending the zip file to Dropbox {e}')
                raise e
        
        file_paths = dbu.FileNeedUpload(args.upload_path, args.file_path)
        update_history_rows = []
        for path, new_name in file_paths:
            print(path, '=>', path, 'on Dropbox', '=>', new_name)
            if args.zip:
                try:
                    dbu.ZipFile(path, './temp.zip')
                    meta = dbu.UpLoadFile(args.upload_path, './temp.zip', new_name + '.zip')
                    if isinstance(meta, dropbox.files.FileMetadata):
                        os.remove('./temp.zip')
                except Exception as e:
                    print('Error happen', e)
            else:
                meta = dbu.UpLoadFile(args.upload_path, path, new_name)
            if isinstance(meta, dropbox.files.FileMetadata):
                update_history_rows.append({
                    'id': meta.id, 'original_name': path.split('/')[-1], 
                    'new_name': meta.name, 
                    # 'hash': dbu.FileHash('./temp.zip'), 
                    'hash': meta.content_hash,
                    'server_modified': meta.server_modified})
            else:
                print('Upload not successfully.')
        if update_history_rows:
            meta = dbu.UpdateHistory(args.upload_path, update_history_rows)
            if isinstance(meta, dropbox.files.FileMetadata):
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
                if isinstance(meta, dropbox.files.FileMetatdata):
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
