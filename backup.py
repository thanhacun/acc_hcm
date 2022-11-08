#!/usr/bin/python
from up_to_dropbox import dropbox_upload_file
import sys
from os import walk, remove

# get files list
backup_path = './backups'
# filenames = next(walk(backup_path), [])[2]

# get the newest files list
new_filenames = next(walk(backup_path), [])[2]
# new_filenames = list(set(new_filenames) - set(filenames))

# upload to dropbox
for filename in new_filenames:
    try:
        print('===== Uploading ', filename, 'to Dropbox... as', filename, '=====')
        dropbox_upload_file(backup_path, filename, '/{}'.format(filename))
        # print('===== Deleting ', filename, '=====')
        # remove(backup_path + '/' + filename)
    except Exception as e:
        print('===== Error happen', e)
