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

## HCMC site (sadmin@192.168.1.18, ~/acc_hcm) — same repo, set up 2026-11-18
- Updated with `git pull` from GitHub. Not in git, copy by hand: `.age_recipient` (same public key as HN -> one private key restores both), `.hc_url` (HCMC has its OWN healthchecks check, different from HN), `.env_dropbox`, `.smb`.
- `age` is not in apt there (Ubuntu 20.04, no sudo): static binary v1.2.1 in `~/.local/bin/age`, so cron sets PATH.
- `run_backup.sh` is configured by env vars (defaults = HN): `BACKUP_DIR`, `MOUNT_DIR`, `UPLOAD_PATH`, `DELETE`.
- Cron (server is UTC; 14:10 UTC = 21:10 Vietnam):
  `10 14 * * 2-6 cd /home/sadmin/acc_hcm && PATH=$HOME/.local/bin:$PATH BACKUP_DIR=./backups/KINDEN60 MOUNT_DIR=./backups UPLOAD_PATH=/KINDEN60 DELETE=0 ./run_backup.sh`
- `DELETE=0`: files stay on the share (//192.168.1.19/backup). Logs: `~/acc_hcm/logs/YYYYMM.log`.
- The Odoo DB backup (`~/backup_odoo.sh`, 15:10 UTC) is a separate job. The old HN cron line that SSHed into HCMC is commented out.
- First run verified 2026-11-18: `/KINDEN60/EFF_KINDEN_HCM202611.effs.zip.age`, HC ping confirmed.
