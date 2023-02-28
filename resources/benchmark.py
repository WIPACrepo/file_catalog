#!/usr/bin/env python3

# fmt:off

from __future__ import print_function

import argparse
import hashlib
from json import dumps as json_encode
from json import loads as json_decode
import random
import string
import time
from typing import Any, List

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
    def __init__(self, url: str, timeout: int = 60):
        self.url: str = url
        self.timeout = timeout
        self.session: Any = requests.Session()

    def _getfileurl(self, uid: str) -> str:
        for _ in range(5):
            try:
                r = self.session.get(self.url + '/api/files',
                                     params={'query': json_encode({'uid': uid})},
                                     timeout=self.timeout)
            except requests.exceptions.Timeout:
                continue
            if r.status_code == 429:
                continue
            r.raise_for_status()
            files: List[str] = json_decode(r.text)['files']
            break
        else:
            raise Exception('server is too busy')
        if len(files) != 1:
            raise KeyError()
        return self.url + files[0]

    def __getitem__(self, uid: str) -> Any:
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

    def __setitem__(self, uid: str, value: Any) -> None:
        meta = value.copy()
        meta['uid'] = uid
        data = json_encode(meta)
        try:
            url = self._getfileurl(uid)
        except KeyError:
            # does not exist
            method = self.session.post
            url = self.url + '/api/files'
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

    def __delitem__(self, uid: str) -> None:
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


def sha512sum(data: bytearray) -> str:
    m = hashlib.sha512()
    m.update(data)
    return m.hexdigest()


def make_data() -> bytearray:
    data = ''.join(random.choice(string.ascii_letters) for _ in range(random.randint(1, 1000)))
    return bytearray(data, 'utf-8')


def benchmark(address: str, num: int) -> float:
    start = time.time()
    fc = FileCatalogLowLevel(address)
    for i in range(num):
        data = make_data()
        cksm = sha512sum(data)
        fc[str(i)] = {'data': data, 'checksum': cksm, 'locations': [make_data()]}
    for i in range(num):
        _ = fc[str(i)]
    for i in range(num):
        del fc[str(i)]
    return time.time() - start


def main() -> None:
    parser = argparse.ArgumentParser(description='benchmark file_catalog server')
    parser.add_argument('--address', type=str, default='http://localhost:8888', help='server address')
    parser.add_argument('-n', '--num', type=int, default=10000, help='number of test values')
    args = parser.parse_args()

    print('starting benchmark')
    t = benchmark(args.address, args.num)
    print('finished. took', t, 'seconds')


if __name__ == '__main__':
    main()
