# server.py
"""File Catalog REST Server Interface."""

# fmt: off
# pylint: disable=R0913,R0903

import datetime
import logging
import os
import secrets
import sys
from pkgutil import get_loader
from typing import Any, Callable, Dict, Optional, cast
from uuid import uuid1

from rest_tools.server import keycloak_role_auth, RestHandler, RestHandlerSetup, RestServer
from tornado.escape import json_decode, json_encode
from tornado.web import HTTPError

from . import argbuilder, deconfliction, urlargparse
from .mongo import Mongo
from .schema import types
from .schema.validation import Validation

logger = logging.getLogger(__name__)

StrDict = Dict[str, Any]

CONFIG_LOGGING_DENY_LIST = ["FC_COOKIE_SECRET", "MONGODB_AUTH_PASS"]

FC_AUTH_PREFIX = "resource_access.file-catalog.roles"
FC_AUTH_ROLES = ["system"]


# --------------------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------------------

if 'CI_TEST_ENV' in os.environ:
    def fc_auth(**_auth: Any) -> Callable[..., Any]:
        def make_wrapper(method: Callable[..., Any]) -> Any:
            async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
                # warn the user about authentication being disabled in testing
                logger.warning("TESTING: auth disabled")
                # go ahead and run the handler
                return await method(self, *args, **kwargs)
            return wrapper
        return make_wrapper
else:
    fc_auth = keycloak_role_auth


# --------------------------------------------------------------------------------------
# Utils
# --------------------------------------------------------------------------------------

def get_pkgdata_filename(package: str, resource: str) -> Optional[str]:
    """Get a filename for a resource bundled within the package."""
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
    fname = mod.__file__
    if not fname:
        return None
    parts.insert(0, os.path.dirname(fname))
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


def set_last_modification_date(metadata: types.Metadata) -> None:
    """Set the `"meta_modify_date"` field."""
    metadata['meta_modify_date'] = str(datetime.datetime.utcnow())


# --------------------------------------------------------------------------------------
# Server Setup
# --------------------------------------------------------------------------------------

def create(config: Dict[str, Any],
           mongo: Mongo,
           port: int = 8888,
           debug: bool = False) -> RestServer:
    """Create an instance of the File Catalog server."""
    for key in config:
        if key not in CONFIG_LOGGING_DENY_LIST:
            logger.info(f"config: {key} => '{config[key]}'")
        else:
            logger.info(f"config: {key} => 'REDACTED'")
    logger.info(f"port: {port}")
    logger.info(f"debug: {debug}")

    static_path = get_pkgdata_filename('file_catalog', 'data/www')
    if static_path is None:
        raise Exception('bad static path')

    template_path = get_pkgdata_filename('file_catalog', 'data/www_templates')
    if template_path is None:
        raise Exception('bad template path')

    handler_setup = {
        "auth": {
            "audience": config["AUTH_AUDIENCE"],
            "openid_url": config["AUTH_OPENID_URL"],
        },
        "config": config,
        "db": mongo,
        "debug": debug,
    }
    if 'CI_TEST_ENV' in os.environ:
        del handler_setup["auth"]
    args = RestHandlerSetup(handler_setup)  # type: ignore
    args["base_url"] = "/api"
    args["config"] = config
    args["db"] = mongo

    cookie_secret = secrets.token_hex(32)  # 32 bytes = 256-bits
    if 'FC_COOKIE_SECRET' in config:
        cookie_secret = config['FC_COOKIE_SECRET']
    else:
        logger.error("FC_COOKIE_SECRET not supplied; random 256-bit secret will not be saved")

    server = RestServer(cookie_secret=cookie_secret,
                        debug=debug,
                        log_function=tornado_logger,
                        login_url='/login',
                        static_path=static_path,
                        template_path=template_path,
                        xsrf_cookies=True)  # type: ignore[no-untyped-call]

    server.add_route(r"/api",                                        HATEOASHandler,                         args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251

    server.add_route(r"/api/collections",                            CollectionsHandler,                     args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251
    server.add_route(r"/api/collections/([^\/]+)",                   SingleCollectionHandler,                args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251
    server.add_route(r"/api/collections/([^\/]+)/files",             SingleCollectionFilesHandler,           args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251
    server.add_route(r"/api/collections/([^\/]+)/snapshots",         SingleCollectionSnapshotsHandler,       args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251

    server.add_route(r"/api/files",                                  FilesHandler,                           args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251
    server.add_route(r"/api/files/count",                            FilesCountHandler,                      args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251
    server.add_route(r"/api/files/([^\/]+)",                         SingleFileHandler,                      args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251
    server.add_route(r"/api/files/([^\/]+)/actions/remove_location", SingleFileActionsRemoveLocationHandler, args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251
    server.add_route(r"/api/files/([^\/]+)/locations",               SingleFileLocationsHandler,             args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251

    server.add_route(r"/api/snapshots/([^\/]+)",                     SingleSnapshotHandler,                  args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251
    server.add_route(r"/api/snapshots/([^\/]+)/files",               SingleSnapshotFilesHandler,             args)  # type: ignore[no-untyped-call]  # noqa: E221, E241, E251

    port = config["FC_PORT"]
    server.startup(port=port)  # type: ignore[no-untyped-call]

    return server


# --------------------------------------------------------------------------------------
# API Routes - the canonical/used File Catalog
# --------------------------------------------------------------------------------------

class APIHandler(RestHandler):
    """Base class for API REST handlers."""

    def initialize(  # type: ignore[override]  # pylint: disable=W0201,W0221
        self,
        config: Dict[str, Any],
        db: Optional[Mongo] = None,
        base_url: str = "/",
        **kwargs: Any,
    ) -> None:
        """Initialize handler."""
        super().initialize(**kwargs)  # type: ignore[no-untyped-call]

        if db is None:
            raise Exception('Mongo instance is None: `db`')
        self.db = db
        self.base_url = base_url
        self.config = config
        self.validation = Validation(self.config)

    def check_xsrf_cookie(self) -> None:  # noqa: D102
        pass

    def set_default_headers(self) -> None:  # noqa: D102
        self.set_header('Content-Type', 'application/hal+json; charset=UTF-8')


# --------------------------------------------------------------------------------------


class HATEOASHandler(APIHandler):
    """Initialize a new handler."""

    def initialize(self, **kwargs: Any) -> None:  # type: ignore[override]  # pylint: disable=C0116,W0221
        """Initialize handler."""
        super().initialize(**kwargs)

        # response is known ahead of time, so pre-compute it
        # pylint: disable=W0201
        self.data = {
            '_links': {
                'self': {'href': self.base_url},
            },
            'files': {'href': os.path.join(self.base_url, 'files')},
        }

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self) -> None:
        """Handle Handle GET requests."""
        self.write(self.data)


# --------------------------------------------------------------------------------------


class FilesHandler(APIHandler):
    """Initialize a handler for requesting files without a known uuid."""

    def initialize(self, **kwargs: Any) -> None:  # type: ignore[override]  # pylint: disable=C0116,W0221
        """Initialize handler."""
        super().initialize(**kwargs)
        # pylint: disable=W0201
        self.files_url = os.path.join(self.base_url, 'files')

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self) -> None:
        """Handle GET requests."""
        try:
            kwargs = urlargparse.parse(self.request.query)
            argbuilder.build_limit(kwargs, self.config)
            argbuilder.build_start(kwargs)
            argbuilder.build_files_query(kwargs)
            argbuilder.build_keys(kwargs)
        except Exception:  # pylint: disable=W0703
            logging.warning('query parameter error', exc_info=True)
            raise HTTPError(400, reason='Invalid query parameter(s)')

        files = await self.db.find_files(**kwargs)

        self.write({
            '_links': {
                'self': {'href': self.files_url},
                'parent': {'href': self.base_url},
            },
            'files': files,
        })

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def post(self) -> None:
        """Handle POST request."""
        metadata: types.Metadata = json_decode(self.request.body)

        # allow user-specified uuid, create if not found
        if 'uuid' not in metadata:
            metadata['uuid'] = str(uuid1())

        # Validate Incoming Data
        if self.validation.has_forbidden_fields_creation(self, metadata):
            return
        if not self.validation.validate_metadata_schema_typing(self, metadata):
            return

        # Deconflict with DB Records
        # NOTE - POST should not conflict with any existing record
        # NOTE - by uuid, by existing location(s), or by existing file-version
        if await self.db.get_file({'uuid': metadata['uuid']}):
            raise HTTPError(
                409,
                reason='Conflict with existing file (uuid already exists)',
                file=os.path.join(self.files_url, metadata['uuid'])
            )
        try:  # check if `metadata` will conflict with an existing metadata record
            if await deconfliction.FileVersion(metadata).is_in_db(self):
                return
        except deconfliction.IndeterminateFileVersionError:
            raise HTTPError(400, reason="File-version cannot be detected from the given 'metadata'")
        if await deconfliction.any_location_in_db(self, metadata.get("locations")):
            return

        # Create & Write-Back
        set_last_modification_date(metadata)
        await self.db.create_file(metadata)
        self.set_status(201)
        self.write({
            '_links': {
                'self': {'href': self.files_url},
                'parent': {'href': self.base_url},
            },
            'file': os.path.join(self.files_url, metadata['uuid']),
        })


# --------------------------------------------------------------------------------------


class FilesCountHandler(APIHandler):
    """Initialize a handler for counting files."""

    def initialize(self, **kwargs: Any) -> None:  # type: ignore[override]  # pylint: disable=C0116,W0221
        """Initialize handler."""
        super().initialize(**kwargs)
        # pylint: disable=W0201
        self.files_url = os.path.join(self.base_url, 'files')

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self) -> None:
        """Handle GET request."""
        try:
            kwargs = urlargparse.parse(self.request.query)
            argbuilder.build_files_query(kwargs)
        except Exception:  # pylint: disable=W0703
            logging.warning('query parameter error', exc_info=True)
            raise HTTPError(400, reason='Invalid query parameter(s)')

        files = await self.db.count_files(**kwargs)

        self.write({
            '_links': {
                'self': {'href': self.files_url},
                'parent': {'href': self.base_url},
            },
            'files': files,
        })


# --------------------------------------------------------------------------------------


class SingleFileHandler(APIHandler):
    """Initialize a handler for requesting single files via uuid."""

    def initialize(self, **kwargs: Any) -> None:  # type: ignore[override]  # pylint: disable=C0116,W0221
        """Initialize handler."""
        super().initialize(**kwargs)
        # pylint: disable=W0201
        self.files_url = os.path.join(self.base_url, 'files')

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self, uuid: str) -> None:
        """Handle GET request."""
        db_file = await self.db.get_file({'uuid': uuid})
        if not db_file:
            raise HTTPError(404, reason='File uuid not found')

        db_file['_links'] = {
            'self': {'href': os.path.join(self.files_url, uuid)},
            'parent': {'href': self.files_url},
        }
        self.write(cast(StrDict, db_file))

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def delete(self, uuid: str) -> None:
        """Handle DELETE request."""
        try:
            await self.db.delete_file({'uuid': uuid})
        except Exception:  # pylint: disable=W0703
            raise HTTPError(404, reason='File uuid not found')
        else:
            self.set_status(204)

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def patch(self, uuid: str) -> None:
        """Handle PATCH request."""
        metadata: types.Metadata = json_decode(self.request.body)

        # Find Matching File
        db_file = await self.db.get_file({'uuid': uuid})
        if not db_file:
            raise HTTPError(404, reason='File uuid not found')

        # Validate Incoming Metadata
        if self.validation.has_forbidden_fields_modification(self, metadata, db_file):
            return

        # Deconflict with DB Records
        # NOTE - PATCH should not conflict with any existing record (excl. uuid's record)
        # NOTE - by existing location(s) or by existing file-version
        if await deconfliction.any_location_in_db(self, metadata.get("locations"), skip=uuid):
            return
        try:
            if await deconfliction.FileVersion(metadata).is_in_db(self, skip=uuid):
                return
        except deconfliction.IndeterminateFileVersionError:
            pass

        # Modify & Write Back
        set_last_modification_date(metadata)
        db_file.update(metadata)
        # we have to validate `db_file` b/c `metadata` may not have all the required fields
        if not self.validation.validate_metadata_schema_typing(self, db_file):
            return
        db_file = await self.db.update_file(uuid, metadata)
        db_file['_links'] = {
            'self': {'href': os.path.join(self.files_url, uuid)},
            'parent': {'href': self.files_url},
        }
        self.write(cast(StrDict, db_file))

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def put(self, uuid: str) -> None:
        """Handle PUT request."""
        metadata: types.Metadata = json_decode(self.request.body)
        metadata['uuid'] = uuid

        # Find Matching File
        db_file = await self.db.get_file({'uuid': uuid})
        if not db_file:
            raise HTTPError(404, reason='File uuid not found')

        # Validate Incoming Metadata
        if self.validation.has_forbidden_fields_modification(self, metadata, db_file):
            return
        if not self.validation.validate_metadata_schema_typing(self, metadata):
            return

        # Deconflict with DB Records
        # NOTE - PUT should not conflict with any existing record (excl. uuid's record)
        # NOTE - by existing location(s) or by existing file-version
        if await deconfliction.any_location_in_db(self, metadata.get("locations"), skip=uuid):
            return
        try:
            if await deconfliction.FileVersion(metadata).is_in_db(self, skip=uuid):
                return
        except deconfliction.IndeterminateFileVersionError:
            # `validate_metadata_schema_typing()` should have detected this anyways
            raise HTTPError(400, reason="File-version cannot be detected from the given 'metadata'")

        # Replace & Write Back
        set_last_modification_date(metadata)
        await self.db.replace_file(metadata.copy())
        metadata['_links'] = {
            'self': {'href': os.path.join(self.files_url, uuid)},
            'parent': {'href': self.files_url},
        }
        self.write(cast(StrDict, metadata))


# --------------------------------------------------------------------------------------


class SingleFileActionsRemoveLocationHandler(APIHandler):
    """Initialize a non-RESTful action handler for removing an existing record's location.

    And potentially the entire record.
    """

    def initialize(self, **kwargs: Any) -> None:  # type: ignore[override]  # pylint: disable=C0116,W0221
        """Initialize handler."""
        super().initialize(**kwargs)
        # pylint: disable=W0201
        self.files_url = os.path.join(self.base_url, 'files')

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def post(self, uuid: str) -> None:
        """Handle POST request.

        Remove location from the record identified by the provided UUID,
        and potentially the entire record.
        """
        # try to load the record from the file catalog by UUID
        db_file = await self.db.get_file({'uuid': uuid})
        if not db_file:
            raise HTTPError(404, reason='File uuid not found')

        # decode the JSON provided in the POST body
        body = json_decode(self.request.body)
        try:
            site = body.pop("site")
            path = body.pop("path")
        except KeyError:
            raise HTTPError(400, reason="POST body requires 'site' & 'path' fields")

        if body:
            # REASONING:
            # If client defines (site=X, path=Y, archive=True)
            # - does this match (site=X, path=Y)?
            # - or (site=X, path=Y, archive=False)?
            # - or only (site=X, path=Y, archive=True)?
            # What if they don't define archive at all? (site=X, path=Y)
            # - does this match (site=X, path=Y, archive=True)?
            # It's unclear, so better fail fast
            raise HTTPError(
                400,
                reason=f"Extra POST body fields detected: {list(body.keys())} "
                       f"('site' & 'path' are required)"
            )

        def is_location_match(loc: types.LocationEntry) -> bool:
            # only match against the mandatory fields
            return bool(loc['site'] == site and loc['path'] == path)

        # Remove `location` (possibly entire record) & Send Back
        before = db_file.get('locations', [])
        after = [loc for loc in before if not is_location_match(loc)]
        # bad location! -- no location was filtered out
        if before == after:
            raise HTTPError(
                404,
                reason=f"Location entry not found for site='{site}' & path='{path}'"
            )
        # remove location! -- there are remaining locations after filtering
        elif after:
            db_file = await self.db.update_file(uuid, {'locations': after})
            # send the record back to the caller
            db_file['_links'] = {
                'self': {'href': os.path.join(self.files_url, uuid)},
                'parent': {'href': self.files_url},
            }
            self.write(cast(StrDict, db_file))
            return
        # delete whole record! -- no remaining locations after filtering
        else:
            await self.db.delete_file({'uuid': uuid})
            # send back empty dict to show record was deleted
            self.write({})
            return


# --------------------------------------------------------------------------------------


class SingleFileLocationsHandler(APIHandler):
    """Initialize a handler for adding new locations to an existing record."""

    def initialize(self, **kwargs: Any) -> None:  # type: ignore[override]  # pylint: disable=C0116,W0221
        """Initialize handler."""
        super().initialize(**kwargs)
        # pylint: disable=W0201
        self.files_url = os.path.join(self.base_url, 'files')

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def post(self, uuid: str) -> None:
        """Handle POST request.

        Add location(s) to the record identified by the provided UUID.
        """
        # try to load the record from the file catalog by UUID
        db_file = await self.db.get_file({'uuid': uuid})
        if not db_file:
            raise HTTPError(404, reason='File uuid not found')

        # decode the JSON provided in the POST body
        metadata: types.Metadata = json_decode(self.request.body)
        locations = metadata.get("locations")

        # if the user didn't provide locations
        if locations is None:
            raise HTTPError(400, reason="POST body requires 'locations' field")

        # validate `locations`
        if not self.validation.is_valid_location_list(locations):
            raise HTTPError(400, reason=self.validation.INVALID_LOCATIONS_LIST_MESSAGE)

        # for each location provided
        new_locations = []
        async for loc, check in deconfliction.find_each_location_in_db(self.db, locations):
            # if we got a file by that location
            if check:
                # if the file we got isn't the one we're trying to update
                if check['uuid'] != uuid:
                    # then that location belongs to another file (already exists)
                    deconfliction.send_location_conflict_error(self, loc, check['uuid'])
                    return
                # note that if we get the record that we are trying to update
                # the location will NOT be added to the list of new_locations
                # which leaves new_locations as a vetted list of addable locations
            # this is a new location
            else:
                # so add it to our list of new locations
                new_locations.append(loc)

        # if there are new locations to append, update the file in the database
        if new_locations:
            db_file = await self.db.append_distinct_elements_to_file(
                uuid, {"locations": new_locations}
            )

        # send the record back to the caller
        db_file['_links'] = {
            'self': {'href': os.path.join(self.files_url, uuid)},
            'parent': {'href': self.files_url},
        }
        self.write(cast(StrDict, db_file))


# --------------------------------------------------------------------------------------
# Collections (unused)
# --------------------------------------------------------------------------------------

class CollectionBaseHandler(APIHandler):
    """Initialize an abstract/base handler for collection-type requests."""

    def initialize(self, **kwargs: Any) -> None:  # type: ignore[override]  # pylint: disable=C0116,W0221
        """Initialize handler."""
        super().initialize(**kwargs)
        # pylint: disable=W0201
        self.collections_url = os.path.join(self.base_url, 'collections')
        self.snapshots_url = os.path.join(self.base_url, 'snapshots')


# --------------------------------------------------------------------------------------


class CollectionsHandler(CollectionBaseHandler):
    """Initialize a handler for collection requests."""

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self) -> None:
        """Handle GET request."""
        try:
            kwargs = urlargparse.parse(self.request.query)
            argbuilder.build_limit(kwargs, self.config)
            argbuilder.build_start(kwargs)
            argbuilder.build_keys(kwargs)
        except Exception:  # pylint: disable=W0703
            logging.warning('query parameter error', exc_info=True)
            raise HTTPError(400, reason='Invalid query parameter(s)')

        collections = await self.db.find_collections(**kwargs)

        self.write({
            '_links': {
                'self': {'href': self.collections_url},
                'parent': {'href': self.base_url},
            },
            'collections': collections,
        })

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def post(self) -> None:
        """Handle POST request."""
        metadata = json_decode(self.request.body)

        try:
            argbuilder.build_files_query(metadata)
            metadata['query'] = json_encode(metadata['query'])
        except Exception:  # pylint: disable=W0703
            logging.warning('query parameter error', exc_info=True)
            raise HTTPError(400, reason='Invalid query parameter(s)')

        if 'collection_name' not in metadata:
            raise HTTPError(400, reason='Missing collection_name')
        if 'owner' not in metadata:
            raise HTTPError(400, reason='Missing owner')

        # allow user-specified uuid, create if not found
        if 'uuid' not in metadata:
            metadata['uuid'] = str(uuid1())

        set_last_modification_date(metadata)
        metadata['creation_date'] = metadata['meta_modify_date']

        ret = await self.db.get_collection({'uuid': metadata['uuid']})

        if ret:
            # collection uuid already exists
            raise HTTPError(409, reason='Conflict with existing collection (uuid already exists)',
                            file=os.path.join(self.collections_url, ret['uuid']))
        else:
            uuid = await self.db.create_collection(metadata)
            self.set_status(201)
        self.write({
            '_links': {
                'self': {'href': self.collections_url},
                'parent': {'href': self.base_url},
            },
            'collection': os.path.join(self.collections_url, uuid),
        })


# --------------------------------------------------------------------------------------


class SingleCollectionHandler(CollectionBaseHandler):
    """Initialize a handler for single collection requests."""

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self, uid: str) -> None:
        """Handle GET request."""
        ret = await self.db.get_collection({'uuid': uid})
        if not ret:
            ret = await self.db.get_collection({'collection_name': uid})

        if ret:
            ret['_links'] = {
                'self': {'href': os.path.join(self.collections_url, uid)},
                'parent': {'href': self.collections_url},
            }

            self.write(ret)
        else:
            raise HTTPError(404, reason='Collection not found')


# --------------------------------------------------------------------------------------


class SingleCollectionFilesHandler(CollectionBaseHandler):
    """Initialize a handler for requesting a single collection's files."""

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self, uid: str) -> None:
        """Handle GET request."""
        ret = await self.db.get_collection({'uuid': uid})
        if not ret:
            ret = await self.db.get_collection({'collection_name': uid})

        if ret:
            try:
                kwargs = urlargparse.parse(self.request.query)
                argbuilder.build_limit(kwargs, self.config)
                argbuilder.build_start(kwargs)
                kwargs['query'] = json_decode(ret['query'])
                argbuilder.build_keys(kwargs)
            except Exception:  # pylint: disable=W0703
                logging.warning('query parameter error', exc_info=True)
                raise HTTPError(400, reason='Invalid query parameter(s)')

            files = await self.db.find_files(**kwargs)

            self.write({
                '_links': {
                    'self': {'href': os.path.join(self.collections_url, uid, 'files')},
                    'parent': {'href': os.path.join(self.collections_url, uid)},
                },
                'files': files,
            })
        else:
            raise HTTPError(404, reason='Collection not found')


# --------------------------------------------------------------------------------------


class SingleCollectionSnapshotsHandler(CollectionBaseHandler):
    """Initialize a handler for requesting a single collection's snapshots."""

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self, uid: str) -> None:
        """Handle GET request."""
        ret = await self.db.get_collection({'uuid': uid})
        if not ret:
            ret = await self.db.get_collection({'collection_name': uid})
        if not ret:
            raise HTTPError(400, reason='Cannot find collection')

        try:
            kwargs = urlargparse.parse(self.request.query)
            argbuilder.build_limit(kwargs, self.config)
            argbuilder.build_start(kwargs)
            argbuilder.build_keys(kwargs)
            kwargs['query'] = {'collection_id': ret['uuid']}
        except Exception:  # pylint: disable=W0703
            logging.warning('query parameter error', exc_info=True)
            raise HTTPError(400, reason='Invalid query parameter(s)')

        snapshots = await self.db.find_snapshots(**kwargs)

        self.write({
            '_links': {
                'self': {'href': os.path.join(self.collections_url, uid, 'snapshots')},
                'parent': {'href': os.path.join(self.collections_url, uid)},
            },
            'snapshots': snapshots,
        })

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def post(self, uid: str) -> None:
        """Handle POST request."""
        ret = await self.db.get_collection({'uuid': uid})
        if not ret:
            ret = await self.db.get_collection({'collection_name': uid})
        if not ret:
            raise HTTPError(400, reason='Cannot find collection')

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

        snapshot = await self.db.get_snapshot({'uuid': metadata['uuid']})

        if snapshot:
            # snapshot uuid already exists
            raise HTTPError(409, reason='Conflict with existing snapshot (uuid already exists)')
        else:
            # find the list of files
            files = await self.db.find_files(**files_kwargs)
            metadata['files'] = [row['uuid'] for row in files]
            logger.warning('creating snapshot %s with files %r', metadata['uuid'], metadata['files'])
            # create the snapshot
            uuid = await self.db.create_snapshot(metadata)
            self.set_status(201)
            self.write({
                '_links': {
                    'self': {'href': os.path.join(self.collections_url, uid, 'snapshots')},
                    'parent': {'href': os.path.join(self.collections_url, uid)},
                },
                'snapshot': os.path.join(self.snapshots_url, uuid),
            })


# --------------------------------------------------------------------------------------
# Snapshots (unused)
# --------------------------------------------------------------------------------------


class SingleSnapshotHandler(CollectionBaseHandler):
    """Initialize a handler for requesting single snapshots."""

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self, uid: str) -> None:
        """Handle GET request."""
        ret = await self.db.get_snapshot({'uuid': uid})

        if ret:
            ret['_links'] = {
                'self': {'href': os.path.join(self.snapshots_url, uid)},
                'parent': {'href': self.collections_url},
            }

            self.write(ret)
        else:
            raise HTTPError(404, reason='Snapshot not found')


# --------------------------------------------------------------------------------------


class SingleSnapshotFilesHandler(CollectionBaseHandler):
    """Initialize a handler for requesting a single snapshot's files."""

    @fc_auth(prefix=FC_AUTH_PREFIX, roles=FC_AUTH_ROLES)
    async def get(self, uid: str) -> None:
        """Handle GET request."""
        ret = await self.db.get_snapshot({'uuid': uid})

        if ret:
            try:
                kwargs = urlargparse.parse(self.request.query)
                argbuilder.build_limit(kwargs, self.config)
                argbuilder.build_start(kwargs)
                kwargs['query'] = {'uuid': {'$in': ret['files']}}
                logger.warning('getting files: %r', kwargs['query'])
                argbuilder.build_keys(kwargs)
            except Exception:  # pylint: disable=W0703
                logging.warning('query parameter error', exc_info=True)
                raise HTTPError(400, reason='Invalid query parameter(s)')

            files = await self.db.find_files(**kwargs)

            self.write({
                '_links': {
                    'self': {'href': os.path.join(self.snapshots_url, uid, 'files')},
                    'parent': {'href': os.path.join(self.snapshots_url, uid)},
                },
                'files': files,
            })
        else:
            raise HTTPError(404, reason='Snapshot not found')
