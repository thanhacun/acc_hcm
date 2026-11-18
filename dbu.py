#!/usr/bin/env python3
"""Upload backup files / folders to Dropbox.

Exit codes: 0 = everything OK, 1 = at least one upload/step failed,
            2 = bad input (missing folder, empty folder with --fail-if-empty).
"""
import argparse
import csv
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from os.path import basename, join
from zipfile import ZIP_DEFLATED, ZipFile

import dropbox
import requests
from dotenv import dotenv_values
from dropbox.files import CommitInfo, FileMetadata, UploadSessionCursor, WriteMode
from tqdm import tqdm

from dropbox_content_hasher import DropboxContentHasher

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORY_NAME = 'history.csv'
HISTORY_FIELDS = ['id', 'original_name', 'new_name', 'hash', 'server_modified']

log = logging.getLogger('dbu')

TRANSIENT = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    dropbox.exceptions.InternalServerError,
    dropbox.exceptions.RateLimitError,
)


def retry(fn, tries=4, base_delay=5):
    """Call fn(); retry with exponential backoff on transient network/API errors."""
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except TRANSIENT as e:
            if attempt == tries:
                raise
            delay = base_delay * 2 ** (attempt - 1)
            log.warning('Transient error (%s), retry %d/%d in %ds', e, attempt, tries - 1, delay)
            time.sleep(delay)


def content_hash(path):
    hasher = DropboxContentHasher()
    with open(path, 'rb') as f:
        while True:
            block = f.read(4 * 1024 * 1024)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


def path_size(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    return sum(os.path.getsize(join(r, n)) for r, _, files in os.walk(path) for n in files)


def make_zip(src, dest_zip):
    """Zip a file (stored by basename) or a folder (stored under its own name)."""
    with ZipFile(dest_zip, 'w', ZIP_DEFLATED, compresslevel=5) as z:
        if os.path.isfile(src):
            z.write(src, arcname=basename(src))
        else:
            parent = os.path.dirname(os.path.abspath(src.rstrip('/')))
            for root, _, files in os.walk(src):
                for name in files:
                    p = join(root, name)
                    z.write(p, os.path.relpath(p, parent))


def check_free_space(directory, needed):
    free = shutil.disk_usage(directory).free
    if free < needed * 1.05 + 100 * 1024 * 1024:
        raise RuntimeError(f'Not enough free space in {directory}: {free >> 20} MB free, ~{needed >> 20} MB needed')


class DropBoxUpload:
    def __init__(self, timeout=900, chunk=8, monthly_mode=False, monthly_regex='', show_pbar=True,
                 env_file=None):
        keys = dotenv_values(env_file or join(BASE_DIR, '.env_dropbox'))
        self.timeout = timeout
        self.chunk = chunk * 1024 * 1024
        self.monthly_mode = monthly_mode  # daily file overwrites the month's file; Dropbox versions keep history
        self.monthly_regex = monthly_regex or r'(.*)(\d{8}).*(\..*)'
        self.show_pbar = show_pbar
        self.dbx = dropbox.Dropbox(app_key=keys['APP_KEY'], app_secret=keys['APP_SECRET'],
                                   oauth2_refresh_token=keys['REFRESH_TOKEN'], timeout=timeout)

    # ---------- upload ----------
    def _upload_once(self, folder, file_path, remote_name):
        dbx = self.dbx
        size = os.path.getsize(file_path)
        dest = f'{folder}/{remote_name}'
        mode = WriteMode('overwrite')
        with open(file_path, 'rb') as f, tqdm(unit='B', unit_scale=True, unit_divisor=1024, total=size,
                                              disable=not self.show_pbar) as pbar:
            if size <= self.chunk:
                meta = dbx.files_upload(f.read(), dest, mode=mode)
                pbar.update(size)
                return meta
            session = dbx.files_upload_session_start(f.read(self.chunk))
            cursor = UploadSessionCursor(session_id=session.session_id, offset=f.tell())
            commit = CommitInfo(path=dest, mode=mode)
            pbar.update(cursor.offset)
            while size - f.tell() > self.chunk:
                dbx.files_upload_session_append_v2(f.read(self.chunk), cursor)
                cursor.offset = f.tell()
                pbar.update(self.chunk)
            return dbx.files_upload_session_finish(f.read(self.chunk), cursor, commit)

    def upload_verified(self, folder, file_path, remote_name):
        """Upload (whole-file retry on transient errors) and verify size + content hash."""
        since = time.time()
        meta = retry(lambda: self._upload_once(folder, file_path, remote_name), tries=3, base_delay=30)
        if not isinstance(meta, FileMetadata):
            raise RuntimeError(f'Unexpected upload result for {remote_name}: {meta!r}')
        if meta.size != os.path.getsize(file_path) or meta.content_hash != content_hash(file_path):
            raise RuntimeError(f'Verification FAILED for {remote_name} (size/hash mismatch)')
        log.info('Uploaded + verified %s/%s (%d MB) in %.0fs', folder, remote_name, meta.size >> 20,
                 time.time() - since)
        return meta

    # ---------- history ----------
    def load_history(self, folder, local_path):
        try:
            retry(lambda: self.dbx.files_download_to_file(local_path, f'{folder}/{HISTORY_NAME}'))
        except dropbox.exceptions.ApiError as e:
            if e.error.is_path() and e.error.get_path().is_not_found():
                log.info('No %s on Dropbox yet', HISTORY_NAME)
                return []
            raise
        with open(local_path, newline='') as f:
            return list(csv.DictReader(f))

    def save_history(self, folder, local_path, rows):
        with open(local_path, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=HISTORY_FIELDS, extrasaction='ignore', restval='')
            w.writeheader()
            w.writerows(rows)
        return self.upload_verified(folder, local_path, HISTORY_NAME)

    def remote_hashes(self, folder, names):
        """Content hashes of remote files (+ revisions of files named in `names`)."""
        hashes = set()
        try:
            res = retry(lambda: self.dbx.files_list_folder(folder))
            entries = list(res.entries)
            while res.has_more:
                res = retry(lambda: self.dbx.files_list_folder_continue(res.cursor))
                entries += res.entries
        except dropbox.exceptions.ApiError as e:
            log.warning('Cannot list %r on Dropbox (%s); assuming nothing uploaded', folder, e)
            return hashes
        for e in entries:
            if not isinstance(e, FileMetadata):
                continue
            hashes.add(e.content_hash)
            if e.name in names:
                try:
                    revs = retry(lambda: self.dbx.files_list_revisions(e.path_lower, limit=100)).entries
                    hashes.update(r.content_hash for r in revs)
                except dropbox.exceptions.ApiError as err:
                    log.warning('Cannot list revisions of %s: %s', e.name, err)
        return hashes

    # ---------- naming / selection ----------
    def MonthlyFileName(self, original_filename):
        """Replace the 8-digit date (YYYYMMDD) by YYYYMM so a month shares one remote file."""
        if not self.monthly_mode:
            return original_filename
        out = ''
        for s in re.split(self.monthly_regex, original_filename):
            if s:
                out += s[:6] if re.match(r'\d{8}', s) else s
        return out

    def files_to_upload(self, folder, local_dir, history, min_age=0):
        """[(local_path, remote_name)] for files not yet in history/Dropbox, oldest name first."""
        now = time.time()
        local = []
        for n in sorted(os.listdir(local_dir)):
            p = join(local_dir, n)
            if not os.path.isfile(p):
                continue
            if now - os.path.getmtime(p) < min_age:
                log.warning('Skip %s: modified < %ds ago (still being written?)', n, min_age)
                continue
            local.append(n)
        hashes = self.remote_hashes(folder, {r['new_name'] for r in history})
        done = {r['original_name'] for r in history if r['hash'] in hashes}
        # sorted order => for monthly mode the newest day is uploaded last and wins
        return [(join(local_dir, n), self.MonthlyFileName(n)) for n in local if n not in done]


# ---------- workflows ----------
def run_single(dbu, args, workdir):
    """Upload one file or folder (zipped) to args.upload_path."""
    src = args.file_path
    if not os.path.exists(src):
        log.error('%s does not exist', src)
        return 2
    name = basename(src.rstrip('/'))
    if args.zip:
        upload_src, remote = join(workdir, 'upload.zip'), name + '.zip'
        check_free_space(workdir, path_size(src))
        log.info('Zipping %s ...', src)
        make_zip(src, upload_src)
    elif os.path.isdir(src):
        log.error('Cannot upload a folder without --zip')
        return 2
    else:
        upload_src, remote = src, name
    dbu.upload_verified(args.upload_path, upload_src, remote)
    if args.delete:
        if os.path.isfile(src):
            os.remove(src)
            log.info('Removed local file %s', src)
        else:
            log.warning('--delete is not supported for folders; nothing removed')
    return 0


def run_files(dbu, args, workdir):
    """Upload every new file of args.file_path, tracking progress in history.csv on Dropbox."""
    local_dir, folder = args.file_path, args.upload_path
    if not os.path.isdir(local_dir):
        log.error('%s is not a folder', local_dir)
        return 2
    if args.fail_if_empty and not any(os.path.isfile(join(local_dir, n)) for n in os.listdir(local_dir)):
        log.error('%s contains no files (share not mounted / nothing copied?)', local_dir)
        return 2

    hist_path = join(workdir, 'history.csv')
    history = dbu.load_history(folder, hist_path)
    pending = dbu.files_to_upload(folder, local_dir, history, args.min_age)
    log.info('%d file(s) to upload', len(pending))
    if args.dry_run:
        for path, remote_name in pending:
            log.info('[dry-run] %s (%d MB) -> %s/%s%s', path, os.path.getsize(path) >> 20, folder,
                     remote_name, '.zip' if args.zip else '')
        log.info('[dry-run] nothing uploaded, nothing deleted')
        return 0

    failures, new_rows = 0, []
    zip_path = join(workdir, 'upload.zip')
    for path, remote_name in pending:
        meta = None  # never reuse the previous file's result
        try:
            if args.zip:
                check_free_space(workdir, os.path.getsize(path))
                log.info('Zipping %s ...', path)
                make_zip(path, zip_path)
                meta = dbu.upload_verified(folder, zip_path, remote_name + '.zip')
            else:
                meta = dbu.upload_verified(folder, path, remote_name)
        except Exception:
            log.exception('FAILED to upload %s', path)
            failures += 1
            continue
        finally:
            if os.path.exists(zip_path):
                os.remove(zip_path)

        new_rows.append({'id': meta.id, 'original_name': basename(path), 'new_name': meta.name,
                         'hash': meta.content_hash, 'server_modified': meta.server_modified})
        if args.delete:  # only reached after a verified upload
            try:
                os.remove(path)
                log.info('Removed local file %s', path)
            except OSError:
                log.exception('Uploaded but could not remove %s', path)
                failures += 1

    if new_rows:
        try:
            dbu.save_history(folder, hist_path, history + new_rows)
            log.info('History updated')
        except Exception:
            log.exception('FAILED to update history')
            failures += 1
    return 1 if failures else 0


def build_parser():
    p = argparse.ArgumentParser()
    p.add_argument('upload_path', type=str, help='path in dropbox, empty if root folder')
    p.add_argument('file_path', type=str, help='file/folder to upload; in folder/monthly mode the folder holding the files')
    p.add_argument('--mode', type=str, default='', help='Upload mode: default, folder or monthly')
    p.add_argument('--zip', action='store_true')
    p.add_argument('--no-zip', dest='zip', action='store_false')
    p.set_defaults(zip=True)
    p.add_argument('--timeout', type=int, default=900)
    p.add_argument('--chunk', type=int, default=8, help='chunk size in MB')
    p.add_argument('--pbar', action='store_true')
    p.add_argument('--no-pbar', dest='pbar', action='store_false')
    p.set_defaults(pbar=True)
    p.add_argument('--delete', action='store_true', help='delete local file after a VERIFIED upload')
    p.add_argument('--no-delete', dest='delete', action='store_false')
    p.set_defaults(delete=False)
    p.add_argument('--dry-run', action='store_true', help='only list what would be uploaded (read-only)')
    p.add_argument('--fail-if-empty', action='store_true', help='exit 2 if the local folder has no files')
    p.add_argument('--min-age', type=int, default=300,
                   help='skip files modified less than N seconds ago (still being written); default 300')
    p.add_argument('--tmp-dir', default=None, help='where to build temporary zip files (needs free space)')
    return p


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
    logging.getLogger('dropbox').setLevel(logging.WARNING)  # hide per-request INFO noise
    args = build_parser().parse_args(argv)
    per_file = args.mode in ('folder', 'monthly') and not (args.mode == 'folder' and args.zip and os.path.isdir(args.file_path))
    workdir = tempfile.mkdtemp(prefix='dbu_', dir=args.tmp_dir)
    try:
        dbu = DropBoxUpload(timeout=args.timeout, chunk=args.chunk, monthly_mode=args.mode == 'monthly',
                            show_pbar=args.pbar)
        return (run_files if per_file else run_single)(dbu, args, workdir)
    except Exception:
        log.exception('Backup failed')
        return 1
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    sys.exit(main())
