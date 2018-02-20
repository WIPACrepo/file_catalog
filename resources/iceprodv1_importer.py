"""
Import file catalog metadata from the IceProd v1 simulation database.
"""

import os
import argparse
import hashlib

import pymysql
import requests

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

def main():
    parser = argparse.ArgumentParser(description='IceProd v1 simulation importer')
    parser.add_argument('--db_host', default=None, help='iceprod db address')
    parser.add_argument('--db_name', default=None, help='iceprod db name')
    parser.add_argument('--db_user', default=None, help='iceprod db user')
    parser.add_argument('--db_passwd', default=None, help='iceprod db password')
    parser.add_argument('--fc_host', default=None, help='file catalog address')
    parser.add_argument('--fc_auth_token', default=None, help='file catalog auth token')
    args = parser.parse_args()

    conn = pymysql.Connection(host=args.db_host, user=args.db_user,
                              passwd=args.db_passwd, db=args.db_name,
                              cursorclass=pymysql.cursors.SSDictCursor)
    cur = conn.cursor()

    s = requests.Session()
    if args.fc_auth_token:
        s.headers.update({'Authorization': 'JWT '+args.fc_auth_token})

    data_template = {
        'data_type':'simulation',
        'content_status':'good',
    }
    fakesha512sum = hashlib.sha512('dummysum').hexdigest()


    sql = """select urlpath.name, urlpath.path, urlpath.dataset_id, urlpath.queue_id,
                    urlpath.md5sum, urlpath.size, job.job_id, job.status_changed from urlpath
             join job on urlpath.dataset_id = job.dataset_id and urlpath.queue_id = job.queue_id
             where job.status="OK"
          """
    cur.execute(sql)
    for row in cur.fetchall_unbuffered():
        name = '/'+row['path'].split('://',1)[-1].split('/',1)[-1]+'/'+row['name']

        # check if existing
        r = s.get(args.fc_host+'/api/files', params={'logical_name':name})
        r.raise_for_status()
        if r.json()['files']:
            print('skipping',name)
            continue

        print('adding',name)
        data = data_template.copy()
        data.update({
            'logical_name': name,
            'locations': [
                {'site': 'WIPAC', 'path': name},
            ],
            'file_size': int(row['size']),
            'checksum': {
                'sha512': fakesha512sum,
                'md5': row['md5sum'],
            },
            'create_date': row['status_changed'].isoformat(),
            'processing_level': get_level(row['path']),
            'iceprod':{
                'dataset': row['dataset_id'],
                'dataset_id': row['dataset_id'],
                'job': row['queue_id'],
                'job_id': row['job_id'],
                'config': 'http://simprod.icecube.wisc.edu/cgi-bin/simulation/cgi/cfg?dataset='+str(row['dataset_id'])+';download=1',
            },
            'simulation': {
                'generator': get_generator(name),
            },
        })
        r = s.post(args.fc_host+'/api/files', json=data)
        r.raise_for_status()

if __name__ == '__main__':
    main()
