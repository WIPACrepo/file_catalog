from __future__ import absolute_import, division, print_function

import sys
import os
import logging
from functools import wraps
from pkgutil import get_loader

import tornado.ioloop
import tornado.web
from tornado.escape import json_encode,json_decode
from tornado.gen import coroutine

import file_catalog
from file_catalog.mongo import Mongo
from file_catalog import urlargparse

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

class Server(object):
    """A file_catalog server instance"""

    def __init__(self, port=8888, db_host='localhost', debug=False):
        static_path = get_pkgdata_filename('file_catalog', 'data/www')
        if static_path is None:
            raise Exception('bad static path')
        template_path = get_pkgdata_filename('file_catalog', 'data/www_templates')
        if template_path is None:
            raise Exception('bad template path')

        main_args = {
            'base_url': '/api',
            'debug': debug,
        }

        api_args = main_args.copy()
        api_args.update({
            'db': Mongo(db_host),
        })

        app = tornado.web.Application([
                (r"/", MainHandler, main_args),
                (r"/api", HATEOASHandler, api_args),
                (r"/api/files", FilesHandler, api_args),
                (r"/api/files/(.*)", SingleFileHandler, api_args),
            ],
            static_path=static_path,
            template_path=template_path,
            log_function=tornado_logger,
        )
        app.listen(port)

    def run(self):
        tornado.ioloop.IOLoop.current().start()

class MainHandler(tornado.web.RequestHandler):
    """Main HTML handler"""
    def initialize(self, base_url='/', debug=False):
        self.base_url = base_url
        self.debug = debug

    def get_template_namespace(self):
        namespace = super(MainHandler,self).get_template_namespace()
        namespace['version'] = file_catalog.__version__
        return namespace

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

class APIHandler(tornado.web.RequestHandler):
    """Base class for API handlers"""
    def initialize(self, db=None, base_url='/', debug=False, rate_limit=10):
        self.db = db
        self.base_url = base_url
        self.debug = debug
        
        # subtract 1 to test before current connection is added
        self.rate_limit = rate_limit-1
        self.rate_limit_data = {}

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
            chunk = json_encode(chunk)
        super(APIHandler, self).write(chunk)

    def write_error(self,status_code=500,**kwargs):
        """Write out custom error page."""
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

    @catch_error
    @coroutine
    def get(self):
        try:
            kwargs = urlargparse.parse(self.request.query)
            if 'limit' in kwargs:
                kwargs['limit'] = int(kwargs['limit'])
                if kwargs['limit'] < 1:
                    raise Exception('limit is not positive')
            if 'start' in kwargs:
                kwargs['start'] = int(kwargs['start'])
                if kwargs['start'] < 0:
                    raise Exception('start is negative')
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
            '_embedded':{
                'files': files,
            },
            'files': [os.path.join(self.files_url,f['_id']) for f in files],
        })

    @catch_error
    @coroutine
    def post(self):
        metadata = json_decode(self.request.body)
        ret = yield self.db.get_file({'file_name':metadata['file_name']})
        if ret:
            # file already exists, check checksum
            if ret['checksum'] != metadata['checksum']:
                self.send_error(409, message='conflict with existing file',
                                file=os.path.join(self.files_url,ret['_id']))
                return
            else:
                # add replica
                ret['locations'].extend(metadata['locations'])
                yield self.db.update_file(ret)
                self.set_status(200)
                ret = ret['_id']
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

    @catch_error
    @coroutine
    def get(self, file_id):
        ret = yield self.db.get_file({'_id':file_id})
        if ret:
            ret['_links'] = {
                'self': {'href': os.path.join(self.files_url,file_id)},
                'parent': {'href': self.files_url},
            }
            self.write(ret)
        else:
            self.send_error(404, message='not found')

    @catch_error
    @coroutine
    def delete(self, file_id):
        try:
            yield self.db.delete_file({'_id':file_id})
        except:
            self.send_error(404, message='not found')
        else:
            self.set_status(204)

    @catch_error
    @coroutine
    def patch(self, file_id):
        metadata = json_decode(self.request.body)
        links = {
            'self': {'href': os.path.join(self.files_url,file_id)},
            'parent': {'href': self.files_url},
        }
        ret = yield self.db.get_file({'_id':file_id})
        if ret:
            # check if this is the same version we're trying to patch
            test_write = ret.copy()
            test_write['_links'] = links
            self.write(test_write)
            self.set_etag_header()
            same = self.check_etag_header()
            self._write_buffer = []
            if same:
                ret.update(metadata)
                yield self.db.update_file(ret.copy())
                ret['_links'] = links
                self.write(ret)
            else:
                self.send_error(409, message='conflict (version mismatch)',
                                _links=links)
        else:
            self.send_error(404, message='not found')

    @catch_error
    @coroutine
    def put(self, file_id):
        metadata = json_decode(self.request.body)
        if '_id' not in metadata:
            metadata['_id'] = file_id
        links = {
            'self': {'href': os.path.join(self.files_url,file_id)},
            'parent': {'href': self.files_url},
        }
        ret = yield self.db.get_file({'_id':file_id})
        if ret:
            # check if this is the same version we're trying to patch
            test_write = ret.copy()
            test_write['_links'] = links
            self.write(test_write)
            self.set_etag_header()
            same = self.check_etag_header()
            self._write_buffer = []
            if same:
                yield self.db.update_file(metadata.copy())
                metadata['_links'] = links
                self.write(metadata)
            else:
                self.send_error(409, message='conflict (version mismatch)',
                                _links=links)
        else:
            self.send_error(404, message='not found')


