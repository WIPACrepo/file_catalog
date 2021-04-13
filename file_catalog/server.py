"""File Catalog REST Server Interface."""

# fmt: off

from __future__ import absolute_import, division, print_function

import copy
import datetime
import logging
import os
import random
import sys
from collections import OrderedDict
from functools import wraps
from importlib.abc import Loader
from pkgutil import get_loader
from typing import Any, Callable, Dict, Optional, Union, cast
from uuid import uuid1

import pymongo.errors  # type: ignore[import]
import tornado.ioloop
import tornado.web
from rest_tools.server import Auth  # type: ignore[import]
from tornado.escape import json_decode, json_encode
from tornado.gen import coroutine
from tornado.httputil import url_concat

# local imports
import file_catalog
from file_catalog import argbuilder, urlargparse
from file_catalog.mongo import Mongo
from file_catalog.schema.validation import Validation

from . import pathfinder
from .schema import types

logger = logging.getLogger('server')


def get_pkgdata_filename(package: str, resource: str) -> Optional[str]:
    """Get a filename for a resource bundled within the package."""
    loader = cast(Optional[Loader], get_loader(package))
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


def tornado_logger(handler: Any) -> None:
    """Log levels based on status code."""
    if handler.get_status() < 400:
        log_method = logger.debug
    elif handler.get_status() < 500:
        log_method = logger.warning
    else:
        log_method = logger.error
    request_time = 1000.0 * handler.request.request_time()
    log_method("%d %s %.2fms", handler.get_status(),
            handler._request_summary(), request_time)


def sort_dict(dict_: Dict[str, Any]) -> 'OrderedDict[str, Any]':
    """Create an OrderedDict by taking `dict` (`dict_`) and orders its keys.

    If a key contains a `dict` it will call this function recursively.
    """
    odict = OrderedDict(sorted(dict_.items()))

    # check for dicts in values
    for key in odict:
        if isinstance(odict[key], dict):
            odict[key] = sort_dict(odict[key])

    return odict


def set_last_modification_date(metadata: types.Metadata) -> None:
    """Set the `"meta_modify_date"` field."""
    metadata['meta_modify_date'] = str(datetime.datetime.utcnow())


# --------------------------------------------------------------------------------------


class Server(object):
    """A file_catalog server instance."""

    def __init__(self, config, port=8888, debug=False,
                 db_host='localhost', db_port=27017,
                 db_auth_source='admin', db_user=None, db_pass=None, db_uri=None) -> None:
        static_path = get_pkgdata_filename('file_catalog', 'data/www')
        if static_path is None:
            raise Exception('bad static path')
        template_path = get_pkgdata_filename('file_catalog', 'data/www_templates')
        if template_path is None:
            raise Exception('bad template path')

        logger.info('db host: %s', db_host)
        logger.info('db port: %s', db_port)
        logger.info('db auth source: %s', db_auth_source)
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
            'db': Mongo(host=db_host, port=db_port, authSource=db_auth_source,
                        username=db_user, password=db_pass, uri=db_uri),
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
                (r"/api/files/count", FilesCountHandler, api_args),
                (r"/api/files/([^\/]+)", SingleFileHandler, api_args),
                (r"/api/files/([^\/]+)/locations", SingleFileLocationsHandler, api_args),
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

    def run(self) -> None:
        """Start IO loop."""
        tornado.ioloop.IOLoop.current().start()


# --------------------------------------------------------------------------------------


class MainHandler(tornado.web.RequestHandler):
    """Main HTML handler."""
    def initialize(self, base_url='/', debug=False, config=None) -> None:
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
        namespace = super().get_template_namespace()
        namespace['version'] = file_catalog.__version__
        return namespace

    def get_current_user(self) -> Optional[str]:
        """Get the current user by parsing the token."""
        try:
            token = self.get_secure_cookie('token')
            logger.info('token: %r', token)
            data = self.auth.validate(token, audience=['ANY'])
            self.auth_key = token
            return data['sub']
        except Exception:
            logger.warning('failed auth', exc_info=True)
        return None

    def get(self) -> None:
        """Handle GET requests."""
        try:
            self.render('index.html')
        except Exception as e:
            logger.warning('Error in main handler', exc_info=True)
            message = 'Error generating page.'
            if self.debug:
                message += '\n' + str(e)
            self.send_error(message=message)

    def write_error(self, status_code: int = 500, **kwargs: Any) -> None:
        """Write out custom error page."""
        self.set_status(status_code)
        if status_code >= 500:
            self.write('<h2>Internal Error</h2>')
        else:
            self.write('<h2>Request Error</h2>')
        if 'message' in kwargs:
            self.write('<br />'.join(kwargs['message'].split('\n')))
        self.finish()


# --------------------------------------------------------------------------------------


def catch_error(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to catch and handle errors on api handlers."""
    @wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            logger.warning('Error in api handler', exc_info=True)
            kwargs = {'message':'Internal error in '+self.__class__.__name__}
            if self.debug:
                kwargs['exception'] = str(e)
            self.send_error(**kwargs)
            return None
    return wrapper


class LoginHandler(MainHandler):
    """Login HTML handler."""
    @catch_error
    def get(self) -> None:
        """Handle GET requests."""
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


# --------------------------------------------------------------------------------------


class AccountHandler(MainHandler):
    """Account HTML handler."""
    @catch_error
    def get(self) -> None:
        """Handle Handle GET requests."""
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


# --------------------------------------------------------------------------------------


def validate_auth(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to check auth key on api handlers."""
    @wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if not self.auth: # skip auth if not present
            return method(self, *args, **kwargs)
        try:
            auth_key = self.request.headers['Authorization'].split(' ',1)
            if not auth_key[0].lower() == 'bearer':
                raise Exception('not a bearer token')
            # logger.info('validate_auth token: %r', auth_key[1])
            self.auth.validate(auth_key[1], audience=['ANY'])
            self.auth_key = auth_key[1]
        except Exception as e:
            logger.warning('auth error', exc_info=True)
            kwargs = {'message':'Authorization error','status_code':403}
            if self.debug:
                kwargs['exception'] = str(e)
            self.send_error(**kwargs)
            return None
        else:
            return method(self, *args, **kwargs)
    return wrapper


class APIHandler(tornado.web.RequestHandler):
    """Base class for API handlers."""
    def initialize(self, config, db=None, base_url='/', debug=False, rate_limit=10) -> None:
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

    def check_xsrf_cookie(self) -> None:
        pass

    def set_default_headers(self) -> None:
        self.set_header('Content-Type', 'application/hal+json; charset=UTF-8')

    def prepare(self) -> None:
        # implement rate limiting
        ip = self.request.remote_ip
        if ip in self.rate_limit_data:
            if self.rate_limit_data[ip] > self.rate_limit:
                self.send_error(429, 'rate limit exceeded for IP address')
            else:
                self.rate_limit_data[ip] += 1
        else:
            self.rate_limit_data[ip] = 1

    def on_finish(self) -> None:
        ip = self.request.remote_ip
        self.rate_limit_data[ip] -= 1
        if self.rate_limit_data[ip] <= 0:
            del self.rate_limit_data[ip]

    def write(self, chunk: Union[str, bytes, Dict[str, Any], types.Metadata]) -> None:
        """Write chunk to output buffer."""
        # override write so we don't output a json header
        if isinstance(chunk, dict):
            chunk = cast(Dict[str, Any], chunk)  # unfortunately necessary, but a no-op
            chunk = sort_dict(chunk)
            chunk = json_encode(chunk)
        super().write(chunk)

    def write_error(self, status_code: int = 500, **kwargs: Any) -> None:
        """Write out custom error page."""
        if 'reason' in kwargs:
            logger.debug('%r',kwargs['reason'])
        self.set_status(status_code)
        kwargs.pop('exc_info',None)
        if kwargs:
            self.write(kwargs)
        self.finish()


# --------------------------------------------------------------------------------------


class HATEOASHandler(APIHandler):
    def initialize(self, **kwargs: Any) -> None:
        super().initialize(**kwargs)

        # response is known ahead of time, so pre-compute it
        self.data = {
            '_links':{
                'self': {'href': self.base_url},
            },
            'files': {'href': os.path.join(self.base_url,'files')},
        }

    @catch_error
    def get(self) -> None:
        self.write(self.data)


# --------------------------------------------------------------------------------------


class FilesHandler(APIHandler):
    def initialize(self, **kwargs: Any) -> None:
        super().initialize(**kwargs)
        self.files_url = os.path.join(self.base_url,'files')
        self.validation = Validation(self.config)

    @validate_auth
    @catch_error
    @coroutine
    def get(self) -> None:
        """Handle GET requests."""
        try:
            kwargs = urlargparse.parse(self.request.query)
            argbuilder.build_limit(kwargs, self.config)
            argbuilder.build_start(kwargs)
            argbuilder.build_files_query(kwargs)
            argbuilder.build_keys(kwargs)
        except:
            logging.warning('query parameter error', exc_info=True)
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
    async def post(self) -> None:
        """Handle POST request."""
        metadata: types.Metadata = json_decode(self.request.body)

        # allow user-specified uuid, create if not found
        if 'uuid' not in metadata:
            metadata['uuid'] = str(uuid1())

        if not self.validation.validate_metadata_creation(self, metadata):
            return

        set_last_modification_date(metadata)
        if await pathfinder.contains_existing_filepaths(self, metadata):
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


# --------------------------------------------------------------------------------------


class FilesCountHandler(APIHandler):
    def initialize(self, **kwargs: Any) -> None:
        super().initialize(**kwargs)
        self.files_url = os.path.join(self.base_url,'files')
        self.validation = Validation(self.config)

    @validate_auth
    @catch_error
    @coroutine
    def get(self) -> None:
        """Handle GET request."""
        try:
            kwargs = urlargparse.parse(self.request.query)
            argbuilder.build_files_query(kwargs)
        except:
            logging.warning('query parameter error', exc_info=True)
            self.send_error(400, message='invalid query parameters')
            return

        files = yield self.db.count_files(**kwargs)

        self.write({
            '_links':{
                'self': {'href': self.files_url},
                'parent': {'href': self.base_url},
            },
            'files': files,
        })


# --------------------------------------------------------------------------------------


class SingleFileHandler(APIHandler):
    def initialize(self, **kwargs: Any) -> None:
        super().initialize(**kwargs)
        self.files_url = os.path.join(self.base_url,'files')
        self.validation = Validation(self.config)

    @validate_auth
    @catch_error
    @coroutine
    def get(self, uuid: str) -> None:
        """Handle GET request."""
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
    def delete(self, uuid: str) -> None:
        """Handle DELETE request."""
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
    async def patch(self, uuid: str) -> None:
        """Handle PATCH request."""
        metadata: types.Metadata = json_decode(self.request.body)

        # Find Matching File
        try:
            db_file = yield self.db.get_file({'uuid': uuid})
        except pymongo.errors.InvalidId:
            self.send_error(400, message='Not a valid uuid')
            return
        if not db_file:
            self.send_error(404, message='not found')
            return

        # Validate Metadata
        if self.validation.has_forbidden_attributes_modification(self, metadata, db_file):
            return
        if await pathfinder.contains_existing_filepaths(self, metadata, uuid=uuid):
            return

        # Add to Metadata
        set_last_modification_date(metadata)

        # Validate Updated Metadata
        db_file.update(metadata)
        # we have to validate `db_file` b/c `metadata` may not have all the required fields
        if not self.validation.validate_metadata_modification(self, db_file):
            return

        # Insert into DB & Write Back
        yield self.db.update_file(uuid, metadata)
        db_file['_links'] = {
            'self': {'href': os.path.join(self.files_url, uuid)},
            'parent': {'href': self.files_url},
        }
        self.write(db_file)


    @validate_auth
    @catch_error
    @coroutine
    async def put(self, uuid: str) -> None:
        """Handle PUT request."""
        metadata: types.Metadata = json_decode(self.request.body)

        # Find Matching File
        try:
            db_file = yield self.db.get_file({'uuid': uuid})
        except pymongo.errors.InvalidId:
            self.send_error(400, message='Not a valid uuid')
            return
        if not db_file:
            self.send_error(404, message='not found')
            return

        # Validate Metadata
        if self.validation.has_forbidden_attributes_modification(self, metadata, db_file):
            return
        if await pathfinder.contains_existing_filepaths(self, metadata, uuid=uuid):
            return
        if not self.validation.validate_metadata_modification(self, metadata):
            return

        # Add to Metadata
        metadata['uuid'] = uuid
        set_last_modification_date(metadata)

        # Insert into DB & Write Back
        yield self.db.replace_file(metadata.copy())
        metadata['_links'] = {
            'self': {'href': os.path.join(self.files_url, uuid)},
            'parent': {'href': self.files_url},
        }
        self.write(metadata)


# --------------------------------------------------------------------------------------


class SingleFileLocationsHandler(APIHandler):
    """Initialize a handler for adding new locations to an existing record."""

    def initialize(self, **kwargs: Any) -> None:
        """Initialize a handler for adding new locations to existing record."""
        super().initialize(**kwargs)
        self.files_url = os.path.join(self.base_url, 'files')

    @validate_auth
    @catch_error
    @coroutine
    def post(self, uuid: str) -> None:
        """Handle POST request.

        Add location(s) to the record identified by the provided UUID.
        """
        # try to load the record from the file catalog by UUID
        try:
            ret = yield self.db.get_file({'uuid': uuid})
        except pymongo.errors.InvalidId:
            self.send_error(400, message='invalid uuid in POST url')
            return

        # if we didn't get a record
        if not ret:
            self.send_error(404, message='record not found in file catalog')
            return

        # decode the JSON provided in the POST body
        metadata: types.Metadata = json_decode(self.request.body)
        locations = metadata.get("locations")

        # if the user didn't provide locations
        if locations is None:
            self.send_error(400, message="POST body requires 'locations' field")
            return

        # if locations isn't a list
        if not isinstance(locations, list):
            self.send_error(400, message="field 'locations' must be an array")
            return

        # for each location provided
        new_locations = []
        for loc in locations:
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
                # note that if we get the record that we are trying to update
                # the location will NOT be added to the list of new_locations
                # which leaves new_locations as a vetted list of addable locations
            # this is a new location
            else:
                # so add it to our list of new locations
                new_locations.append(loc)

        # if there are new locations to append
        if new_locations:
            # update the file in the database
            yield self.db.append_distinct_elements_to_file(uuid, {'locations': new_locations})
            # re-read the updated file from the database
            ret = yield self.db.get_file({'uuid': uuid})

        # send the record back to the caller
        ret['_links'] = {
            'self': {'href': os.path.join(self.files_url, uuid)},
            'parent': {'href': self.files_url},
        }
        self.write(ret)


### Collections ###
# --------------------------------------------------------------------------------------


class CollectionBaseHandler(APIHandler):
    def initialize(self, **kwargs) -> None:
        super().initialize(**kwargs)
        self.collections_url = os.path.join(self.base_url,'collections')
        self.snapshots_url = os.path.join(self.base_url,'snapshots')


# --------------------------------------------------------------------------------------


class CollectionsHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self) -> None:
        """Handle GET request."""
        try:
            kwargs = urlargparse.parse(self.request.query)
            argbuilder.build_limit(kwargs, self.config)
            argbuilder.build_start(kwargs)
            argbuilder.build_keys(kwargs)
        except:
            logging.warning('query parameter error', exc_info=True)
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
    def post(self) -> None:
        """Handle POST request."""
        metadata = json_decode(self.request.body)

        try:
            argbuilder.build_files_query(metadata)
            metadata['query'] = json_encode(metadata['query'])
        except:
            logging.warning('query parameter error', exc_info=True)
            self.send_error(400, message='invalid query parameters')
            return

        if 'collection_name' not in metadata:
            self.send_error(400, message='missing collection_name')
            return
        if 'owner' not in metadata:
            self.send_error(400, message='missing owner')
            return

        # allow user-specified uuid, create if not found
        if 'uuid' not in metadata:
            metadata['uuid'] = str(uuid1())

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


# --------------------------------------------------------------------------------------


class SingleCollectionHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid: str) -> None:
        """Handle GET request."""
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


# --------------------------------------------------------------------------------------


class SingleCollectionFilesHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid: str) -> None:
        """Handle GET request."""
        ret = yield self.db.get_collection({'uuid':uid})
        if not ret:
            ret = yield self.db.get_collection({'collection_name':uid})

        if ret:
            try:
                kwargs = urlargparse.parse(self.request.query)
                argbuilder.build_limit(kwargs, self.config)
                argbuilder.build_start(kwargs)
                kwargs['query'] = json_decode(ret['query'])
                argbuilder.build_keys(kwargs)
            except:
                logging.warning('query parameter error', exc_info=True)
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


# --------------------------------------------------------------------------------------


class SingleCollectionSnapshotsHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid: str) -> None:
        """Handle GET request."""
        ret = yield self.db.get_collection({'uuid':uid})
        if not ret:
            ret = yield self.db.get_collection({'collection_name':uid})
        if not ret:
            self.send_error(400, message='cannot find collection')
            return

        try:
            kwargs = urlargparse.parse(self.request.query)
            argbuilder.build_limit(kwargs, self.config)
            argbuilder.build_start(kwargs)
            argbuilder.build_keys(kwargs)
            kwargs['query'] = {'collection_id': ret['uuid']}
        except:
            logging.warning('query parameter error', exc_info=True)
            self.send_error(400, message='invalid query parameters')
            return

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
    def post(self, uid: str) -> None:
        """Handle POST request."""
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
            metadata['uuid'] = str(uuid1())

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


# --------------------------------------------------------------------------------------


class SingleSnapshotHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid: str) -> None:
        """Handle GET request."""
        ret = yield self.db.get_snapshot({'uuid':uid})

        if ret:
            ret['_links'] = {
                'self': {'href': os.path.join(self.snapshots_url,uid)},
                'parent': {'href': self.collections_url},
            }

            self.write(ret)
        else:
            self.send_error(404, message='snapshot not found')


# --------------------------------------------------------------------------------------


class SingleSnapshotFilesHandler(CollectionBaseHandler):
    @validate_auth
    @catch_error
    @coroutine
    def get(self, uid: str) -> None:
        """Handle GET request."""
        ret = yield self.db.get_snapshot({'uuid':uid})

        if ret:
            try:
                kwargs = urlargparse.parse(self.request.query)
                argbuilder.build_limit(kwargs, self.config)
                argbuilder.build_start(kwargs)
                kwargs['query'] = {'uuid':{'$in':ret['files']}}
                logger.warning('getting files: %r', kwargs['query'])
                argbuilder.build_keys(kwargs)
            except:
                logging.warning('query parameter error', exc_info=True)
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
