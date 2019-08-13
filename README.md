# file_catalog
Store file metadata information in a file catalog

[![CircleCI](https://circleci.com/gh/WIPACrepo/file_catalog/tree/master.svg?style=shield)](https://circleci.com/gh/WIPACrepo/file_catalog/tree/master)

## Prerequisites
To get the prerequisites necessary for the file catalog:

    sudo pip install futures pymongo tornado ldap3 pyjwt

## Running the server
To start an instance of the server running:

    python -m file_catalog

## Running the unit tests
To run the unit tests for the service, you need the
[CircleCI CLI](https://circleci.com/docs/2.0/local-cli/).
Then run it with:

    circleci local execute --job test

## Configuration
All configuration is done using environment variables.
To get the list of possible configuration parameters and their defaults, run

    python -m file_catalog --show-config-spec

## Interface

The primary interface is an HTTP server. TLS and other security
hardening mechanisms are handled by a reverse proxy server as
for normal web applications.

## Browser

Requests to the main url `/` are browsable like a standard website.
They will use javascript to activate the REST API as necessary.

## REST API

Requests with urls of the form `/api/RESOURCE` can access the
REST API. Responses are in [HAL](http://stateless.co/hal_specification.html)
JSON format.

### Files

Unique identifiers:

* `mongo_id`

  The id that the mongodb generates.

* `uid`

  The `uid` is an unique identifier that can be chosen freely.

Mandatory fields:

*  `uid`

*  `checksum`

    Must be calculated with SHA512.

*  `locations`

    Is a list with at least one non-empty URL to a file location. Can contain more than one location.

#### /api/files

Resource representing the collection of all files in the catalog.

Operations:

* GET: Obtain list of files

  **Query Parameters**

  * limit: (positive integer) number of results to provide
  * start: (non-negative integer) result at which to start at
  * query: (mongodb query) query specification

  The server SHOULD honor the *start* parameter. The server MAY honor the
  *limit* parameter. In cases where the server does not honor the *limit*
  parameter, it should do so by providing fewer resources (*limit* should
  be considered the client’s upper limit for the number of resources in
  the response).

  **Result Codes**

  * 200: Response contains collection of file resources
  * 400: Bad request (query parameters invalid)
  * 429: Too many requests (if server is being hammered)
  * 500: Unspecified server error
  * 503: Service unavailable (maintenance, etc.)

* POST: Create a new file or add a replica

  If a file exists and the checksum is the same, a replica
  is added. If the checksum is different a conflict error is returned.

  **Result Codes**

  * 200: Replica has been added. Response contains link to file resource
  * 201: Response contains link to newly created file resource
  * 400: Bad request (metadata failed validation)
  * 409: Conflict (if the file already exists); includes link to existing file
  * 429: Too many requests (if server is being hammered)
  * 500: Unspecified server error
  * 503: Service unavailable (maintenance, etc.)

* DELETE: Not supported

* PUT: Not supported

* PATCH: Not supported

#### /api/files/{mongo_id}

Resource representing the metadata for a file in the file catalog.

Operations:

* GET: Obtain file metadata information

  **Result Codes**

  * 200: Response contains metadata of file resource
  * 404: Not Found (file resource does not exist)
  * 429: Too many requests (if server is being hammered)
  * 500: Unspecified server error
  * 503: Service unavailable (maintenance, etc.)

* POST: Not supported

* DELETE: Delete the metadata for the file

  **Result Codes**

  * 204: No Content (file resource is successfully deleted)
  * 404: Not Found (file resource does not exist)
  * 429: Too many requests (if server is being hammered)
  * 500: Unspecified server error
  * 503: Service unavailable (maintenance, etc.)

* PUT: Fully update/replace file metadata information

  **Result Codes**

  * 200: Response indicates metadata of file resource has been updated/replaced
  * 404: Not Found (file resource does not exist) + link to “files” resource for POST
  * 409: Conflict (if updating an outdated resource - use ETAG hash to compare)
  * 429: Too many requests (if server is being hammered)
  * 500: Unspecified server error
  * 503: Service unavailable (maintenance, etc.)

* PATCH: Partially update/replace file metadata information

  The JSON provided as body to PATCH need not contain all the
  keys, only the keys that need to be updated. If a key is
  provided with a value null, then that key can be removed from
  the metadata.

  **Result Codes**

  * 200: Response indicates metadata of file resource has been updated/replaced
  * 404: Not Found (file resource does not exist) + link to “files” resource for POST
  * 409: Conflict (if updating an outdated resource - use ETAG hash to compare)
  * 429: Too many requests (if server is being hammered)
  * 500: Unspecified server error
  * 503: Service unavailable (maintenance, etc.)

## Development

### Unit testing
In case it comes in handy, the following command can be used to run
a single unit test. Replace the name of the test as necessary.

    circleci local execute --job test -e PYTEST_ADDOPTS='-s tests/test_files.py -k test_10_files'

Note that for a file to be picked up, it must be added to git first (with git add).
