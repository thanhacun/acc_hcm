* Create app secrets on Dropbox => then get the refresh token
* Mount backups folder with credentials (hide the credential in a file)
* Syntax for running backup
- Normal:
- Folder: python dbu.py '/remote_folder' './local_folder' --mode folder [--no-zip] [--pbar]
- Monthly: python dbu.py '/remote_folder' './local_folder' --mode monthly [--zip] [--no-pbar]
- TODO []: zip and upload a folder

* Create crontab
* Ref:
- https://realpython.com/python-csv/
- https://timlehr.com/auto-mount-samba-cifs-shares-via-fstab-on-linux/
- https://stackoverflow.com/questions/70641660/how-do-you-get-and-use-a-refresh-token-for-the-dropbox-api-python-3-x
- https://jwc-rad.medium.com/how-to-upload-large-files-with-dropbox-api-for-python-5caceb4c7e2f
- https://stackoverflow.com/questions/28522669/how-to-print-the-percentage-of-zipping-a-file-python/41664456#41664456
- https://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse
