#!/usr/bin/env python

# fmt:off

from __future__ import print_function

import hashlib
import random
import string
import time
from json import dumps as json_encode
from json import loads as json_decode

import requests


class FileCatalogLowLevel(object):
    """
    Low level file catalog interface.  Use like a dict::

        fc = FileCatalog('http://file_catalog.com')
        fc['my_new_file'] = {'locations':['/this/is/a/path']}

    Args:
        url (str): url of the file catalog server
        timeout (float): (optional) seconds to wait for a query to finish
    """
    def __init__(self, url, timeout=60):
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()

    def _getfileurl(self, uid):
        for _ in range(5):
            try:
                r = self.session.get(self.url+'/api/files',
                                     params={'query':json_encode({'uid':uid})},
                                     timeout=self.timeout)
            except requests.exceptions.Timeout:
                continue
            if r.status_code == 429:
                continue
            r.raise_for_status()
            files = json_decode(r.text)['files']
            break
        else:
            raise Exception('server is too busy')
        if len(files) != 1:
            raise KeyError()
        return self.url+files[0]

    def __getitem__(self, uid):
        url = self._getfileurl(uid)
        for _ in range(5):
            try:
                r = self.session.get(url, timeout=self.timeout)
            except requests.exceptions.Timeout:
                continue
            if r.status_code == 429:
                continue
            r.raise_for_status()
            return json_decode(r.text)
        raise Exception('server is too busy')

    def __setitem__(self, uid, value):
        meta = value.copy()
        meta['uid'] = uid
        data = json_encode(meta)
        try:
            url = self._getfileurl(uid)
        except KeyError:
            # does not exist
            method = self.session.post
            url = self.url+'/api/files'
        else:
            # exists, so update
            method = self.session.put
        for _ in range(5):
            try:
                r = method(url, data=data,
                           timeout=self.timeout)
            except requests.exceptions.Timeout:
                continue
            if r.status_code == 429:
                continue
            r.raise_for_status()
            return
        raise Exception('server is too busy')

    def __delitem__(self, uid):
        url = self._getfileurl(uid)
        for _ in range(5):
            try:
                r = self.session.delete(url, timeout=self.timeout)
            except requests.exceptions.Timeout:
                continue
            if r.status_code == 429:
                continue
            r.raise_for_status()
            return
        raise Exception('server is too busy')


def sha512sum(data):
    m = hashlib.sha512()
    m.update(data)
    return m.hexdigest()


def make_data():
    return ''.join(random.choice(string.ascii_letters) for _ in range(random.randint(1,1000)))


def benchmark(address,num):
    start = time.time()
    fc = FileCatalogLowLevel(address)
    for i in range(num):
        data = make_data()
        cksm = sha512sum(data)
        fc[str(i)] = {'data':data,'checksum':cksm,'locations':[make_data()]}
    for i in range(num):
        meta = fc[str(i)]
    for i in range(num):
        del fc[str(i)]
    return time.time()-start


def main():
    import argparse
    parser = argparse.ArgumentParser(description='benchmark file_catalog server')
    parser.add_argument('--address',type=str,default='http://localhost:8888',help='server address')
    parser.add_argument('-n','--num',type=int,default=10000,help='number of test values')
    args = parser.parse_args()

    print('starting benchmark')
    t = benchmark(args.address, args.num)
    print('finished. took',t,'seconds')


if __name__ == '__main__':
    main()
