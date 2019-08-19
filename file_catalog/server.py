from __future__ import absolute_import, division, print_function

import copy
import sys
import os
import logging
import random
from functools import wraps
from pkgutil import get_loader
from collections import OrderedDict
import uuid
import datetime

try:
    import urlparse
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlparse, urlencode

import pymongo.errors

import tornado.ioloop
import tornado.web
from tornado.httputil import url_concat
from tornado.escape import json_encode,json_decode
from tornado.gen import coroutine
from tornado.httpclient import HTTPError
from rest_tools.server import Auth

import file_catalog
from file_catalog.mongo import Mongo
from file_catalog import urlargparse
from file_catalog.validation import Validation

logger = logging.getLogger('server')

def get_pkgdata_filename(package, resource):
    """Get a filename for a resource bundled within the package"""
    loader = get_loader(package)
    if loader is None or not hasattr(loader, 'get_data'):
        return None
    mod = sys.modules.get(package) or loader.load_module(package)
    if mod is None or not hasattr(mod, '__file__'):
        return None

    # Modify the resource name to be compatible with the loader.get_data
    # signature - an os.path format "filename" starting with the dirname of
    # the package's __file__
    parts = resource.split('/')
    parts.insert(0, os.path.dirname(mod.__file__))
    return os.path.join(*parts)

def tornado_logger(handler):
    """Log levels based on status code"""
    if handler.get_status() < 400:
        log_method = logger.debug
    elif handler.get_status() < 500:
        log_method = logger.warning
    else:
        log_method = logger.error
    request_time = 1000.0 * handler.request.request_time()
    log_method("%d %s %.2fms", handler.get_status(),
            handler._request_summary(), request_time)

def sort_dict(d):
    """
    Creates an OrderedDict by taking the `dict` named `d` and orders its keys.
    If a key contains a `dict` it will call this function recursively.
    """

    od = OrderedDict(sorted(d.items()))

    # check for dicts in values
    for key in od:
        if isinstance(od[key], dict):
            od[key] = sort_dict(od[key])

    return od

def set_last_modification_date(d):
    d['meta_modify_date'] = str(datetime.datetime.utcnow())

class Server(object):
    """A file_catalog server instance"""

    def __init__(self, config, port=8888, debug=False,
            db_host='localhost', db_port=27017, db_user=None, db_pass=None):
        static_path = get_pkgdata_filename('file_catalog', 'data/www')
        if static_path is None:
            raise Exception('bad static path')
        template_path = get_pkgdata_filename('file_catalog', 'data/www_templates')
        if template_path is None:
            raise Exception('bad template path')

        logger.info('db host: %s', db_host)
        logger.info('db port: %s', db_port)
        logger.info('db user: %s', db_user)
        logger.info('server port: %r', port)
        logger.info('debug: %r', debug)
        redacted_config = copy.deepcopy(config)
        redacted_config['MONGODB_AUTH_PASS'] = 'REDACTED'
        logger.info('redacted config: %r', redacted_config)

        main_args = {
            'base_url': '/api',
            'debug': debug,
            'config': config,
        }

        api_args = main_args.copy()
        api_args.update({
            'db': Mongo(host=db_host, port=db_port, username=db_user, password=db_pass),
            'config': config,
        })

        if config['FC_COOKIE_SECRET'] is not None:
            cookie_secret = config['FC_COOKIE_SECRET']
        else:
            cookie_secret = ''.join(chr(random.randint(0,128)) for _ in range(16))

        app = tornado.web.Application([
                (r"/", MainHandler, main_args),
                (r"/login", LoginHandler, main_args),
                (r"/account", AccountHandler, main_args),
                (r"/api", HATEOASHandler, api_args),
                (r"/api/files", FilesHandler, api_args),
                (r"/api/files/([^\/]+)", SingleFileHandler, api_args),
                (r"/api/collections", CollectionsHandler, api_args),
                (r"/api/collections/([^\/]+)", SingleCollectionHandler, api_args),
                (r"/api/collections/([^\/]+)/files", SingleCollectionFilesHandler, api_args),
                (r"/api/collections/([^\/]+)/snapshots", SingleCollectionSnapshotsHandler, api_args),
                (r"/api/snapshots/([^\/]+)", SingleSnapshotHandler, api_args),
                (r"/api/snapshots/([^\/]+)/files", SingleSnapshotFilesHandler, api_args),
            ],
            static_path=static_path,
            template_path=template_path,
            log_function=tornado_logger,
            login_url='/login',
            xsrf_cookies=True,
            cookie_secret=cookie_secret,
            debug=debug,
        )
        app.listen(port)

    def run(self):
        tornado.ioloop.IOLoop.current().start()

class MainHandler(tornado.web.RequestHandler):
    """Main HTML handler"""
    def initialize(self, base_url='/', debug=False, config=None):
        self.base_url = base_url
        self.debug = debug
        self.config = config
        if 'TOKEN_KEY' in self.config:
            self.auth = Auth(algorithm=self.config['TOKEN_ALGORITHM'],
                                secret=self.config['TOKEN_KEY'],
                                issuer=self.config['TOKEN_URL'])
            self.auth_key = None
        else:
            self.auth = None
        self.current_user_secure = None
        self.address = config['FC_PUBLIC_URL']

    def get_template_namespace(self):
        namespace = super(MainHandler,self).get_template_namespace()
        namespace['version'] = file_catalog.__version__
        return namespace

    def get_current_user(self):
        try:
            token = self.get_secure_cookie('token')
            logger.info('token: %r', token)
            data = self.auth.validate(token, audience=['ANY'])
            self.auth_key = token
            return data['sub']
        except Exception:
            logger.warn('failed auth', exc_info=True)
            pass
        return None

    def get(self):
        try:
            self.render('index.html')
        except Exception as e:
            logger.warn('Error in main handler', exc_info=True)
            message = 'Error generating page.'
            if self.debug:
                message += '\n' + str(e)
            self.send_error(message=message)

    def write_error(self,status_code=500,**kwargs):
        """Write out custom error page."""
        self.set_status(status_code)
        if status_code >= 500:
            self.write('<h2>Internal Error</h2>')
        else:
            self.write('<h2>Request Error</h2>')
        if 'message' in kwargs:
            self.write('<br />'.join(kwargs['message'].split('\n')))
        self.finish()

def catch_error(method):
    """Decorator to catch and handle errors on api handlers"""
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            logger.warn('Error in api handler', exc_info=True)
            kwargs = {'message':'Internal error in '+self.__class__.__name__}
            if self.debug:
                kwargs['exception'] = str(e)
            self.send_error(**kwargs)
    return wrapper

class LoginHandler(MainHandler):
    """Login HTML handler"""
    @catch_error
    def get(self):
        if not self.get_argument('access', False):
            url = url_concat(self.config['TOKEN_URL']+'/token', {
                'redirect': self.address + self.request.uri,
                'state': self.get_argument('next', '/'),
                'scope': 'file-catalog',
            })
            logging.info('redirect to %s', url)
            self.redirect(url)
            return

        redirect = self.get_argument('state', '/')
        access = self.get_argument('access')
        self.set_secure_cookie('token', access)
        logging.info('request: %r %r', redirect, access)
        self.redirect(redirect)


class AccountHandler(MainHandler):
    """Account HTML handler"""
    @catch_error
    def get(self):
        if not self.get_argument('access', False):
            url = url_concat(self.config['TOKEN_URL']+'/token', {
                'redirect': self.address + self.request.uri,
                'scope': 'file-catalog',
            })
            self.redirect(url)
            return

        access = self.get_argument('access')
        refresh = self.get_argument('refresh')
        self.render('account.html', authkey=refresh, tempkey=access)

def validate_auth(method):
    """Decorator to check auth key on api handlers"""
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.auth: # skip auth if not present
            return method(self, *args, **kwargs)
        try:
            auth_key = self.request.headers['Authorization'].split(' ',1)
            if not auth_key[0].lower() == 'bearer':
                raise Exception('not a bearer token')
            self.auth.validate(auth_key[1], audience=['ANY'])
            self.auth_key = auth_key[1]
        except Exception as e:
            logger.warn('auth error', exc_info=True)
            kwargs = {'message':'Authorization error','status_code':403}
            if self.debug:
                kwargs['exception'] = str(e)
            self.send_error(**kwargs)
        else:
            return method(self, *args, **kwargs)
    return wrapper

class APIHandler(tornado.web.RequestHandler):
    """Base class for API handlers"""
    def initialize(self, config, db=None, base_url='/', debug=False, rate_limit=10):
        self.db = db
        self.base_url = base_url
        self.debug = debug
        self.config = config
        if 'TOKEN_KEY' in self.config:
            self.auth = Auth(algorithm=self.config['TOKEN_ALGORITHM'],
                                secret=self.config['TOKEN_KEY'],
                                issuer=self.config['TOKEN_URL'])
            self.auth_key = None
        else:
            self.auth = None

        # subtract 1 to test before current connection is added
        self.rate_limit = rate_limit-1
        self.rate_limit_data = {}

    def check_xsrf_cookie(self):
        pass

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/hal+json; charset=UTF-8')

    def prepare(self):
        # implement rate limiting
        ip = self.request.remote_ip
        if ip in self.rate_limit_data:
            if self.rate_limit_data[ip] > self.rate_limit:
                self.send_error(429, 'rate limit exceeded for IP address')
            else:
                self.rate_limit_data[ip] += 1
        else:
            self.rate_limit_data[ip] = 1

    def on_finish(self):
        ip = self.request.remote_ip
        self.rate_limit_data[ip] -= 1
        if self.rate_limit_data[ip] <= 0:
            del self.rate_limit_data[ip]

    def write(self, chunk):
        # override write so we don't output a json header
        if isinstance(chunk, dict):
            chunk = json_encode(sort_dict(chunk))
        super(APIHandler, self).write(chunk)

    def write_error(self,status_code=500,**kwargs):
        """Write out custom error page."""
        if 'reason' in kwargs:
            logger.debug('%r',kwargs['reason'])
        self.set_status(status_code)
        kwargs.pop('exc_info',None)
        if kwargs:
            self.write(kwargs)
        self.finish()

class HATEOASHandler(APIHandler):
    def initialize(self, **kwargs):
        super(HATEOASHandler, self).initialize(**kwargs)

        # response is known ahead of time, so pre-compute it
        self.data = {
            '_links':{
                'self': {'href': self.base_url},
            },
            'files': {'href': os.path.join(self.base_url,'files')},
        }

    @catch_error
    def get(self):
        self.write(self.data)

class FilesHandler(APIHandler):
    def initialize(self, **kwargs):
        super(FilesHandler, self).initialize(**kwargs)
        self.files_url = os.path.join(self.base_url,'files')
        self.validation = Validation(self.config)

    @validate_auth
    @catch_error
    @coroutine
    def get(self):
        try:
            kwargs = urlargparse.parse(self.request.query)
            if 'limit' in kwargs:
                kwargs['limit'] = int(kwargs['limit'])
                if kwargs['limit'] < 1:
                    raise Exception('limit is not positive')

                # check with config
                if kwargs['limit'] > self.config['FC_QUERY_FILE_LIST_LIMIT']:
                    kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']
            else:
                # if no limit has been defined, set max limit
                kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']

            if 'start' in kwargs:
                kwargs['start'] = int(kwargs['start'])
                if kwargs['start'] < 0:
                    raise Exception('start is negative')

            if 'query' in kwargs:
                kwargs['query'] = json_decode(kwargs['query'])
            else:
                kwargs['query'] = {}
            if 'locations.archive' not in kwargs['query']:
                kwargs['query']['locations.archive'] = None

            # shortcut query params
            if 'logical_name' in kwargs:
                kwargs['query']['logical_name'] = kwargs.pop('logical_name')
            if 'run_number' in kwargs:
                kwargs['query']['run_number'] = kwargs.pop('run_number')
            if 'dataset' in kwargs:
                kwargs['query']['iceprod.dataset'] = kwargs.pop('dataset')
            if 'event_id' in kwargs:
                e = kwargs.pop('event_id')
                kwargs['query']['first_event'] = {'$lte': e}
                kwargs['query']['last_event'] = {'$gte': e}
            if 'processing_level' in kwargs:
                kwargs['query']['processing_level'] = kwargs.pop('processing_level')
            if 'season' in kwargs:
                kwargs['query']['offline.season'] = kwargs.pop('season')

            if 'keys' in kwargs:
                kwargs['keys'] = kwargs['keys'].split('|')
        except:
            logging.warn('query parameter error', exc_info=True)
            self.send_error(400, message='invalid query parameters')
            return
        files = yield self.db.find_files(**kwargs)
        self.write({
            '_links':{
                'self': {'href': self.files_url},
                'parent': {'href': self.base_url},
            },
            'files': files,
        })

    @validate_auth
    @catch_error
    @coroutine
    def post(self):
        metadata = json_decode(self.request.body)

        # allow user-specified uuid, create if not found
        if 'uuid' not in metadata:
            metadata['uuid'] = str(uuid.uuid1())

        if not self.validation.validate_metadata_creation(self, metadata):
            return

        set_last_modification_date(metadata)

        # try to find an existing document by this logical_name
        ret = yield self.db.get_file({'logical_name': metadata['logical_name']})

        if ret:
            # the logical_name already exists
            self.send_error(409, message='conflict with existing file (logical_name already exists)',
                            file=os.path.join(self.files_url, ret['uuid']))
            return

        # for each provided location
        for loc in metadata['locations']:
            # try to find an existing document by this location
            ret = yield self.db.get_file({'locations': {'$elemMatch': loc}})
            # if we are able to find an existing document
            if ret:
                # the location already exists
                self.send_error(409, message='conflict with existing file (location already exists)',
                                file=os.path.join(self.files_url, ret['uuid']),
                                location=loc)
                return

        ret = yield self.db.get_file({'uuid':metadata['uuid']})

        if ret:
            # file uuid already exists, check checksum
            if ret['checksum'] != metadata['checksum']:
                # the uuid already exists (no replica since checksum is different
                self.send_error(409, message='conflict with existing file (uuid already exists)',
                                file=os.path.join(self.files_url,ret['uuid']))
                return
            elif any(f in ret['locations'] for f in metadata['locations']):
                # replica has already been added
                self.send_error(409, message='replica has already been added',
                                file=os.path.join(self.files_url,ret['uuid']))
                return
            else:
                # add replica
                ret['locations'].extend(metadata['locations'])

                yield self.db.update_file(ret['uuid'], {'locations': ret['locations']})
                self.set_status(200)
                ret = ret['uuid']
        else:
            ret = yield self.db.create_file(metadata)
            self.set_status(201)
        self.write({
            '_links':{
                'self': {'href': self.files_url},
                'parent': {'href': self.base_url},
            },
            'file': os.path.join(self.files_url, ret),
        })

class SingleFileHandler(APIHandler):
    def initialize(self, **kwargs):
        super(SingleFileHandler, self).initialize(**kwargs)
        self.files_url = os.path.join(self.base_url,'files')
        self.validation = Validation(self.config)

    @validate_auth
    @catch_error
    @coroutine
    def get(self, uuid):
        try:
            ret = yield self.db.get_file({'uuid':uuid})

            if ret:
                ret['_links'] = {
                    'self': {'href': os.path.join(self.files_url,uuid)},
                    'parent': {'href': self.files_url},
                }

                self.write(ret)
            else:
                self.send_error(404, message='not found')
        except pymongo.errors.InvalidId:
            self.send_error(400, message='Not a valid uuid')

    @validate_auth
    @catch_error
    @coroutine
    def delete(self, uuid):
        try:
            yield self.db.delete_file({'uuid':uuid})
        except pymongo.errors.InvalidId:
            self.send_error(400, message='Not a valid uuid')
        except:
            self.send_error(404, message='not found')
        else:
            self.set_status(204)

    @validate_auth
    @catch_error
    @coroutine
    def patch(self, uuid):
        metadata = json_decode(self.request.body)

        links = {
            'self': {'href': os.path.join(self.files_url,uuid)},
            'parent': {'href': self.files_url},
        }

        try:
            ret = yield self.db.get_file({'uuid':uuid})
        except pymongo.errors.InvalidId:
            self.send_error(400, message='Not a valid uuid')
            return

        if self.validation.has_forbidden_attributes_modification(self, metadata, ret):
            return

        # if the user provided a logical_name
        if 'logical_name' in metadata:
            # try to load a file by that logical_name
            check = yield self.db.get_file({'logical_name': metadata['logical_name']})
            # if we got a file by that logical_name
            if check:
                # if the file we got isn't the one we're trying to update
                if check['uuid'] != uuid:
                    # then that logical_name belongs to another file (already exists)
                    self.send_error(409, message='conflict with existing file (logical_name already exists)',
                                    file=os.path.join(self.files_url, check['uuid']))
                    return

        # if the user provided locations
        if 'locations' in metadata:
            # for each location provided
            for loc in metadata['locations']:
                # try to load a file by that location
                check = yield self.db.get_file({'locations': {'$elemMatch': loc}})
                # if we got a file by that location
                if check:
                    # if the file we got isn't the one we're trying to update
                    if check['uuid'] != uuid:
                        # then that location belongs to another file (already exists)
                        self.send_error(409, message='conflict with existing file (location already exists)',
                                        file=os.path.join(self.files_url, check['uuid']),
                                        location=loc)
                        return

        set_last_modification_date(metadata)

        if ret:
            ret.update(metadata)
            if not self.validation.validate_metadata_modification(self, ret):
                return

            yield self.db.update_file(uuid, metadata)
            ret['_links'] = links
            self.write(ret)
        else:
            self.send_error(404, message='not found')

    @validate_auth
    @catch_error
    @coroutine
    def put(self, uuid):
        metadata = json_decode(self.request.body)

        links = {
            'self': {'href': os.path.join(self.files_url,uuid)},
            'parent': {'href': self.files_url},
        }

        try:
            ret = yield self.db.get_file({'uuid':uuid})
        except pymongo.errors.InvalidId:
            self.send_error(400, message='Not a valid uuid')
            return

        if self.validation.has_forbidden_attributes_modification(self, metadata, ret):
            return

        # if the user provided a logical_name
        if 'logical_name' in metadata:
            # try to load a file by that logical_name
            check = yield self.db.get_file({'logical_name': metadata['logical_name']})
            # if we got a file by that logical_name
            if check:
                # if the file we got isn't the one we're trying to update
                if check['uuid'] != uuid:
                    # then that logical_name belongs to another file (already exists)
                    self.send_error(409, message='conflict with existing file (logical_name already exists)',
                                    file=os.path.join(self.files_url, check['uuid']))
                    return

        # if the user provided locations
        if 'locations' in metadata:
            # for each location provided
            for loc in metadata['locations']:
                # try to load a file by that location
                check = yield self.db.get_file({'locations': {'$elemMatch': loc}})
                # if we got a file by that location
                if check:
                    # if the file we got isn't the one we're trying to update
                    if check['uuid'] != uuid:
                        # then that location belongs to another file (already exists)
                        self.send_error(409, message='conflict with existing file (location already exists)',
                                        file=os.path.join(self.files_url, check['uuid']),
                                        location=loc)
                        return

        metadata['uuid'] = uuid
        set_last_modification_date(metadata)

        if ret:
            if not self.validation.validate_metadata_modification(self, metadata):
                return

            yield self.db.replace_file(metadata.copy())
            metadata['_links'] = links
            self.write(metadata)
        else:
            self.send_error(404, message='not found')


### Collections ###

class CollectionBaseHandler(APIHandler):
    def initialize(self, **kwargs):
        super(CollectionBaseHandler, self).initialize(**kwargs)
        self.collections_url = os.path.join(self.base_url,'collections')
        self.snapshots_url = os.path.join(self.base_url,'snapshots')

class CollectionsHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self):
        try:
            kwargs = urlargparse.parse(self.request.query)
            if 'limit' in kwargs:
                kwargs['limit'] = int(kwargs['limit'])
                if kwargs['limit'] < 1:
                    raise Exception('limit is not positive')

                # check with config
                if kwargs['limit'] > self.config['FC_QUERY_FILE_LIST_LIMIT']:
                    kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']
            else:
                # if no limit has been defined, set max limit
                kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']

            if 'start' in kwargs:
                kwargs['start'] = int(kwargs['start'])
                if kwargs['start'] < 0:
                    raise Exception('start is negative')

            if 'keys' in kwargs:
                kwargs['keys'] = kwargs['keys'].split('|')
        except:
            logging.warn('query parameter error', exc_info=True)
            self.send_error(400, message='invalid query parameters')
            return
        collections = yield self.db.find_collections(**kwargs)
        self.write({
            '_links':{
                'self': {'href': self.collections_url},
                'parent': {'href': self.base_url},
            },
            'collections': collections,
        })

    @validate_auth
    @catch_error
    @coroutine
    def post(self):
        metadata = json_decode(self.request.body)

        query = {}
        try:
            if 'query' in metadata:
                query = metadata.pop('query')
            if 'locations.archive' not in query:
                query['locations.archive'] = None

            # shortcut query params
            if 'logical_name' in metadata:
                query['logical_name'] = metadata.pop('logical_name')
            if 'run_number' in metadata:
                query['run_number'] = metadata.pop('run_number')
            if 'dataset' in metadata:
                query['iceprod.dataset'] = metadata.pop('dataset')
            if 'event_id' in metadata:
                e = metadata.pop('event_id')
                query['first_event'] = {'$lte': e}
                query['last_event'] = {'$gte': e}
            if 'processing_level' in metadata:
                query['processing_level'] = metadata.pop('processing_level')
            if 'season' in metadata:
                query['offline.season'] = metadata.pop('season')

        except:
            logging.warn('query parameter error', exc_info=True)
            self.send_error(400, message='invalid query parameters')
            return
        metadata['query'] = json_encode(query)

        if 'collection_name' not in metadata:
            self.send_error(400, message='missing collection_name')
            return
        if 'owner' not in metadata:
            self.send_error(400, message='missing owner')
            return

        # allow user-specified uuid, create if not found
        if 'uuid' not in metadata:
            metadata['uuid'] = str(uuid.uuid1())

        set_last_modification_date(metadata)
        metadata['creation_date'] = metadata['meta_modify_date']

        ret = yield self.db.get_collection({'uuid':metadata['uuid']})

        if ret:
            # collection uuid already exists
            self.send_error(409, message='conflict with existing file (uuid already exists)',
                            file=os.path.join(self.files_url,ret['uuid']))
            return
        else:
            ret = yield self.db.create_collection(metadata)
            self.set_status(201)
        self.write({
            '_links':{
                'self': {'href': self.collections_url},
                'parent': {'href': self.base_url},
            },
            'collection': os.path.join(self.collections_url, ret),
        })

class SingleCollectionHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid):
        ret = yield self.db.get_collection({'uuid':uid})
        if not ret:
            ret = yield self.db.get_collection({'collection_name':uid})

        if ret:
            ret['_links'] = {
                'self': {'href': os.path.join(self.collections_url,uid)},
                'parent': {'href': self.collections_url},
            }

            self.write(ret)
        else:
            self.send_error(404, message='collection not found')

class SingleCollectionFilesHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid):
        ret = yield self.db.get_collection({'uuid':uid})
        if not ret:
            ret = yield self.db.get_collection({'collection_name':uid})

        if ret:
            try:
                kwargs = urlargparse.parse(self.request.query)
                if 'limit' in kwargs:
                    kwargs['limit'] = int(kwargs['limit'])
                    if kwargs['limit'] < 1:
                        raise Exception('limit is not positive')

                    # check with config
                    if kwargs['limit'] > self.config['FC_QUERY_FILE_LIST_LIMIT']:
                        kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']
                else:
                    # if no limit has been defined, set max limit
                    kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']

                if 'start' in kwargs:
                    kwargs['start'] = int(kwargs['start'])
                    if kwargs['start'] < 0:
                        raise Exception('start is negative')

                kwargs['query'] = json_decode(ret['query'])

                if 'keys' in kwargs:
                    kwargs['keys'] = kwargs['keys'].split('|')
            except:
                logging.warn('query parameter error', exc_info=True)
                self.send_error(400, message='invalid query parameters')
                return
            files = yield self.db.find_files(**kwargs)
            self.write({
                '_links':{
                    'self': {'href': os.path.join(self.collections_url,uid,'files')},
                    'parent': {'href': os.path.join(self.collections_url,uid)},
                },
                'files': files,
            })
        else:
            self.send_error(404, message='collection not found')

class SingleCollectionSnapshotsHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid):
        ret = yield self.db.get_collection({'uuid':uid})
        if not ret:
            ret = yield self.db.get_collection({'collection_name':uid})
        if not ret:
            self.send_error(400, message='cannot find collection')
            return

        try:
            kwargs = urlargparse.parse(self.request.query)
            if 'limit' in kwargs:
                kwargs['limit'] = int(kwargs['limit'])
                if kwargs['limit'] < 1:
                    raise Exception('limit is not positive')

                # check with config
                if kwargs['limit'] > self.config['FC_QUERY_FILE_LIST_LIMIT']:
                    kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']
            else:
                # if no limit has been defined, set max limit
                kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']

            if 'start' in kwargs:
                kwargs['start'] = int(kwargs['start'])
                if kwargs['start'] < 0:
                    raise Exception('start is negative')

            if 'keys' in kwargs:
                kwargs['keys'] = kwargs['keys'].split('|')
        except:
            logging.warn('query parameter error', exc_info=True)
            self.send_error(400, message='invalid query parameters')
            return
        kwargs['query'] = {'collection_id': ret['uuid']}
        snapshots = yield self.db.find_snapshots(**kwargs)
        self.write({
            '_links':{
                'self': {'href': os.path.join(self.collections_url,uid,'snapshots')},
                'parent': {'href': os.path.join(self.collections_url,uid)},
            },
            'snapshots': snapshots,
        })

    @validate_auth
    @catch_error
    @coroutine
    def post(self, uid):
        ret = yield self.db.get_collection({'uuid':uid})
        if not ret:
            ret = yield self.db.get_collection({'collection_name':uid})
        if not ret:
            self.send_error(400, message='cannot find collection')
            return

        files_kwargs = {
            'query': json_decode(ret['query']),
            'keys': ['uuid'],
        }

        if self.request.body:
            metadata = json_decode(self.request.body)
        else:
            metadata = {}

        metadata['collection_id'] = uid
        if 'owner' not in metadata:
            metadata['owner'] = ret['owner']

        # allow user-specified uuid, create if not found
        if 'uuid' not in metadata:
            metadata['uuid'] = str(uuid.uuid1())

        set_last_modification_date(metadata)
        metadata['creation_date'] = metadata['meta_modify_date']
        del metadata['meta_modify_date']

        ret = yield self.db.get_snapshot({'uuid':metadata['uuid']})

        if ret:
            # snapshot uuid already exists
            self.send_error(409, message='conflict with existing snapshot (uuid already exists)')
        else:
            # find the list of files
            files = yield self.db.find_files(**files_kwargs)
            metadata['files'] = [row['uuid'] for row in files]
            logger.warning('creating snapshot %s with files %r', metadata['uuid'], metadata['files'])
            # create the snapshot
            ret = yield self.db.create_snapshot(metadata)
            self.set_status(201)
            self.write({
                '_links':{
                    'self': {'href': os.path.join(self.collections_url,uid,'snapshots')},
                    'parent': {'href': os.path.join(self.collections_url,uid)},
                },
                'snapshot': os.path.join(self.snapshots_url, ret),
            })

class SingleSnapshotHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid):
        ret = yield self.db.get_snapshot({'uuid':uid})

        if ret:
            ret['_links'] = {
                'self': {'href': os.path.join(self.snapshots_url,uid)},
                'parent': {'href': self.collections_url},
            }

            self.write(ret)
        else:
            self.send_error(404, message='snapshot not found')

class SingleSnapshotFilesHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid):
        ret = yield self.db.get_snapshot({'uuid':uid})

        if ret:
            try:
                kwargs = urlargparse.parse(self.request.query)
                if 'limit' in kwargs:
                    kwargs['limit'] = int(kwargs['limit'])
                    if kwargs['limit'] < 1:
                        raise Exception('limit is not positive')

                    # check with config
                    if kwargs['limit'] > self.config['FC_QUERY_FILE_LIST_LIMIT']:
                        kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']
                else:
                    # if no limit has been defined, set max limit
                    kwargs['limit'] = self.config['FC_QUERY_FILE_LIST_LIMIT']

                if 'start' in kwargs:
                    kwargs['start'] = int(kwargs['start'])
                    if kwargs['start'] < 0:
                        raise Exception('start is negative')

                kwargs['query'] = {'uuid':{'$in':ret['files']}}
                logger.warning('getting files: %r', kwargs['query'])

                if 'keys' in kwargs:
                    kwargs['keys'] = kwargs['keys'].split('|')
            except:
                logging.warn('query parameter error', exc_info=True)
                self.send_error(400, message='invalid query parameters')
                return
            files = yield self.db.find_files(**kwargs)
            self.write({
                '_links':{
                    'self': {'href': os.path.join(self.snapshots_url,uid,'files')},
                    'parent': {'href': os.path.join(self.snapshots_url,uid)},
                },
                'files': files,
            })
        else:
            self.send_error(404, message='snapshot not found')
