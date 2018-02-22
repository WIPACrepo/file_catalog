"""
Import file catalog metadata from the IceProd v2 simulation database.
"""

import sys
import os
import argparse
import hashlib

import pymysql
import requests

try:
    from crawler import generate_files, stat
except ImportError:
    print('Requires file_crawler in PYTHONPATH')
    sys.exit(1)

level_types = {
    'detector': ['detector'],
    'L1': ['level1'],
    'L2': ['level2'],
    'L3': ['level3'],
    'L4': ['level4'],
}
def get_level(path):
    """transforn path to processing level"""
    path = path.lower()
    for k in level_types:
        if any(l in path for l in level_types[k]):
            return k
    return 'unknown'

generator_types = {
    'corsika': ['corsika'],
    'nugen': ['nugen','neutrino','numu','nue','nutau'],
}
def get_generator(path):
    """transform path to generator"""
    path = path.lower()
    for k in generator_types:
        if any(g in path for g in generator_types[k]):
            return k
    return 'unknown'

def get_dataset(path):
    """get dataset num"""
    name = path.rsplit('/')[-1]
    for part in name.split('.'):
        try:
            if part.startswith('02') or part.startswith('01'):
                return int(part)
        except Exception:
            continue
    raise Exception('cannot find dataset')

def get_job(path):
    """get job num"""
    name = path.rsplit('/')[-1]
    next = False
    for part in name.split('.'):
        try:
            if next:
                return int(part)
            if part.startswith('02') or part.startswith('01'):
                p = int(part)
                next = True
        except Exception:
            continue
    raise Exception('cannot find job')

def main():
    parser = argparse.ArgumentParser(description='IceProd v2 simulation importer')
    parser.add_argument('--fc_host', default=None, help='file catalog address')
    parser.add_argument('--fc_auth_token', default=None, help='file catalog auth token')
    parser.add_argument('path', help='filesystem path to crawl')
    args = parser.parse_args()

    s = requests.Session()
    if args.fc_auth_token:
        s.headers.update({'Authorization': 'JWT '+args.fc_auth_token})

    data_template = {
        'data_type':'simulation',
        'content_status':'good',
    }
    fakesha512sum = hashlib.sha512('dummysum').hexdigest()

    for name in generate_files(args.path):
        dataset_num = get_dataset(name)
        if dataset_num < 20000:
            continue
        #dataset_id = get_dataset_id(name)
        
        # check if existing
        r = s.get(args.fc_host+'/api/files', params={'logical_name':name})
        r.raise_for_status()
        if r.json()['files']:
            print('skipping',name)
            continue

        print('adding',name)
        row = stat(name)
        data = data_template.copy()
        data.update({
            'logical_name': name,
            'locations': [
                {'site': 'WIPAC', 'path': name},
            ],
            'file_size': int(row['size']),
            'checksum': {
                'sha512': fakesha512sum,
            },
            'create_date': row['ctime'],
            'processing_level': get_level(name),
            'iceprod':{
                'dataset': dataset_num,
                #'dataset_id': dataset_id,
                'job': get_job(name),
                #'job_id': get_job_id(name),
                #'config': 'https://iceprod2.icecube.wisc.edu/config?dataset_id='+str(dataset_id),
            },
            'simulation': {
                'generator': get_generator(name),
            },
        })
        r = s.post(args.fc_host+'/api/files', json=data)
        r.raise_for_status()

if __name__ == '__main__':
    main()
