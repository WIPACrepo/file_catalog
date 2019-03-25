#!/usr/bin/env python
import argparse
from concurrent.futures import ProcessPoolExecutor
import hashlib
import json
import os
import requests
import sys
from time import sleep

def sha512sum(path):
    """Return the SHA512 checksum of the file given by path."""
    bufsize = 4194304
    h = hashlib.new('sha512')
    with open(path, 'rb', buffering=0) as f:
        line = f.read(bufsize)
        while line:
            h.update(line)
            line = f.read(bufsize)
    return h.hexdigest()

def process_dir(path):
    """Return list of sub-directories and metadata of files in directory given by path."""
    try:
        scan = list(os.scandir(path))
    except (PermissionError, FileNotFoundError):
        scan = []
    dirs = []
    file_meta = []
    for dent in scan:
        if dent.is_symlink():
            continue
        elif dent.is_dir():
            dirs.append(dent.path)
        elif dent.is_file():
            f = {'path':dent.path}
            try:
                f['size'] = dent.stat().st_size
            except (PermissionError, FileNotFoundError):
                continue
            try: # OSError is thrown for special files like sockets
                f['sha512'] = sha512sum(dent.path)
            except (OSError, PermissionError, FileNotFoundError):
                continue
            file_meta.append(f)
    return dirs, file_meta

def gather_file_info(dirs):
    """Return an iterator for metadata of files recursively found under dirs."""
    futures = []
    with ProcessPoolExecutor() as pool:
        while futures or dirs:
            for d in dirs:
                futures.append(pool.submit(process_dir, d))
            while not futures[0].done(): # concurrent.futures.wait(FIRST_COMPLETED) is slower
                sleep(0.1)
            future = futures.pop(0)
            dirs,file_meta = future.result()
            yield from file_meta

def main():
    parser = argparse.ArgumentParser(
            description='Find files under PATH(s), compute their metadata and '
                        'upload it to File Catalog.',
            epilog='Notes: (1) symbolic links are never followed.',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('path', metavar='PATH', nargs='+', 
            help='path(s) to scan for files')
    parser.add_argument('-u', '--url', default='http://localhost:8888',
            help='File Catalog URL')
    parser.add_argument('-s', '--site', required=True, 
            help='site value of the "locations" object')
    args = parser.parse_args()
    
    for f in gather_file_info(args.path):
        metadata = {
            'logical_name': f['path'],
            'checksum': {'sha512':f['sha512']},
            'file_size': f['size'],
            'locations': [{'site':args.site, 'path':f['path']}],
        }
        with requests.post(args.url + '/api/files', data=json.dumps(metadata)) as r:
            if r.status_code != 201:
                print('Unexpected return code:', r.status_code, r.reason, r.json())

if __name__ == '__main__':
    sys.exit(main())
