* Create app secrets on Dropbox => then get the refresh token
* Mount backups folder with credentials (hide the credential in a file)
* Syntax for running backup
- Normal:
- Folder: python dbu.py '/remote_folder' './local_folder' --mode folder [--no-zip] [--pbar]
- Monthly: python dbu.py '/remote_folder' './local_folder' --mode monthly [--zip] [--no-pbar]

* Create crontab
* Ref:
- https://realpython.com/python-csv/
- https://timlehr.com/auto-mount-samba-cifs-shares-via-fstab-on-linux/
- https://stackoverflow.com/questions/70641660/how-do-you-get-and-use-a-refresh-token-for-the-dropbox-api-python-3-x
- https://jwc-rad.medium.com/how-to-upload-large-files-with-dropbox-api-for-python-5caceb4c7e2f
- https://stackoverflow.com/questions/28522669/how-to-print-the-percentage-of-zipping-a-file-python/41664456#41664456
- https://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse

## Daily run (cron)
`10 21 * * 2-6 /home/thcn/acc_hanoi/run_backup.sh`  -> logs in `logs/YYYYMM.log`, healthchecks ping (URL in `.hc_url`).
Dry run: `python3 dbu.py '' ./backups --mode monthly --zip --no-pbar --dry-run`

## Encryption (age)
Uploads are zipped, then encrypted with age to the PUBLIC key in `.age_recipient` -> `NAME_YYYYMM.ext.zip.age` on Dropbox.
The PRIVATE key (`age-backup-key.txt`) is NOT on this server: keep it in the password manager + offline copy.
Lose the key = backups cannot be restored.

### Restore
1. Download the `.age` file from Dropbox (Version history keeps ~365 days of daily versions -> pick the date).
2. Decrypt + unzip (on any machine with `age` installed: `sudo apt install age`):
```
age -d -i age-backup-key.txt EFF_KINDEN202611.effs.zip.age > restored.zip
unzip -t restored.zip      # integrity check
unzip restored.zip
```
Test file for practice: Dropbox `/_encryption_test/encryption_test.txt.zip.age` (sha256 of the inner file: b62d79afcad851d4517889ae3e88cc53bce225bcb1b17f60cb568fb4cdb00f80).
Files uploaded before encryption was enabled are plain `.zip` (no decryption needed).
