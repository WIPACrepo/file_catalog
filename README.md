<!--- Top of README Badges (automated) --->
[![PyPI](https://img.shields.io/pypi/v/wipac-file-catalog)](https://pypi.org/project/wipac-file-catalog/) [![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/file_catalog?include_prereleases)](https://github.com/WIPACrepo/file_catalog/) [![PyPI - License](https://img.shields.io/pypi/l/wipac-file-catalog)](https://github.com/WIPACrepo/file_catalog/blob/master/LICENSE) [![Lines of code](https://img.shields.io/tokei/lines/github/WIPACrepo/file_catalog)](https://github.com/WIPACrepo/file_catalog/) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/file_catalog)](https://github.com/WIPACrepo/file_catalog/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/file_catalog)](https://github.com/WIPACrepo/file_catalog/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen) 
<!--- End of README Badges (automated) --->
# file_catalog
Store file metadata information in a file catalog



## Prerequisites
To get the prerequisites necessary for the file catalog:

    pip install -r requirements.txt



## Running the server
To start an instance of the server running:

    python -m file_catalog



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


### File-Entry Fields

#### File-Metadata Schema:
* _See [types.py](https://github.com/WIPACrepo/file_catalog/blob/master/file_catalog/schema/types.py)_

#### Mandatory Fields:
* `uuid` (provided by File Catalog)
* `logical_name`
* `locations` (with at least one non-empty URL)
* `file_size`
* `checksum.sha512`


### Route: `/api/files`
Resource representing the collection of all files in the catalog.


#### Method: `GET`
Obtain list of files

##### REST-Query Parameters
  * [`limit`](#limit)
  * [`start`](#start)
  * [`logical_name`](#shortcut-parameters-logical-name-regex-logical_name-directory-filename) *(shortcut parameter)*
  * [`directory`](#shortcut-parameters-logical-name-regex-logical_name-directory-filename) *(shortcut parameter)*
  * [`filename`](#shortcut-parameters-logical-name-regex-logical_name-directory-filename) *(shortcut parameter)*
  * [`logical-name-regex`](#shortcut-parameters-logical-name-regex-logical_name-directory-filename) *(shortcut parameter)*
  * [`run_number`](#shortcut-parameter-run_number) *(shortcut parameter)*
  * [`dataset`](#shortcut-parameter-dataset) *(shortcut parameter)*
  * [`event_id`](#shortcut-parameter-event_id) *(shortcut parameter)*
  * [`processing_level`](#shortcut-parameter-processing_level) *(shortcut parameter)*
  * [`season`](#shortcut-parameter-season) *(shortcut parameter)*
  * [`query`](#query)
  * [`keys`](#keys)
  * [`all-keys`](#shortcut-parameter-all-keys) *(shortcut parameter)*
  * [`max_time_ms`](#max_time_ms)

##### HTTP Response Status Codes
  * `200`: Response contains collection of file resources
  * `400`: Bad request (query parameters invalid)
  * `429`: Too many requests (if server is being hammered)
  * `500`: Unspecified server error
  * `503`: Service unavailable (maintenance, etc.)

#### Method: `POST`
Create a new file or add a replica

*If a file exists and the checksum is the same, a replica is added. If the checksum is different a conflict error is returned.*

##### REST-Body
  * *See [File-Entry Fields](#File-Entry-Fields)*

##### HTTP Response Status Codes
  * `200`: Replica has been added. Response contains link to file resource
  * `201`: Response contains link to newly created file resource
  * `400`: Bad request (metadata failed validation)
  * `409`: Conflict (if the file-version already exists); includes link to existing file
  * `429`: Too many requests (if server is being hammered)
  * `500`: Unspecified server error
  * `503`: Service unavailable (maintenance, etc.)

#### Method: `DELETE`
*Not supported*

#### Method: `PUT`
*Not supported*

#### Method: `PATCH`
*Not supported*


### Route: `/api/files/{uuid}`
Resource representing the metadata for a file in the file catalog.

#### Method: `GET`
Obtain file metadata information

##### REST-Query Parameters
  * *None*

##### HTTP Response Status Codes
  * `200`: Response contains metadata of file resource
  * `404`: Not Found (file resource does not exist)
  * `429`: Too many requests (if server is being hammered)
  * `500`: Unspecified server error
  * `503`: Service unavailable (maintenance, etc.)

#### Method: `POST`
*Not supported*

#### Method: `DELETE`
Delete the metadata for the file

##### REST-Query Parameters
  * *None*

##### HTTP Response Status Codes
  * `204`: No Content (file resource is successfully deleted)
  * `404`: Not Found (file resource does not exist)
  * `429`: Too many requests (if server is being hammered)
  * `500`: Unspecified server error
  * `503`: Service unavailable (maintenance, etc.)

#### Method: `PUT `
Fully update/replace file metadata information

##### REST-Body
  * *See [File-Entry Fields](#File-Entry-Fields)*

##### HTTP Response Status Codes
  * `200`: Response indicates metadata of file resource has been updated/replaced
  * `404`: Not Found (file resource does not exist) + link to “files” resource for POST
  * `409`: Conflict (if updating an outdated resource - use ETAG hash to compare)
  * `429`: Too many requests (if server is being hammered)
  * `500`: Unspecified server error
  * `503`: Service unavailable (maintenance, etc.)

#### Method: `PATCH`
Partially update/replace file metadata information

*The JSON provided as body to PATCH need not contain all the keys, only the  need to be updated. If a key is provided with a value null, then that key can be removed from the metadata.*

##### REST-Body
  * *See [File-Entry Fields](#File-Entry-Fields)*

##### HTTP Response Status Codes
  * `200`: Response indicates metadata of file resource has been updated/replaced
  * `404`: Not Found (file resource does not exist) + link to “files” resource for POST
  * `409`: Conflict (if updating an outdated resource - use ETAG hash to compare)
  * `429`: Too many requests (if server is being hammered)
  * `500`: Unspecified server error
  * `503`: Service unavailable (maintenance, etc.)


### More About REST-Query Parameters

##### `limit`
- *positive integer;* number of results to provide *(default: 10000)*
- **NOTE:** The server *may* honor the `limit` parameter. In cases where the server does not honor the `limit` parameter, it should do so by providing fewer resources (`limit` should be considered the client’s upper limit for the number of resources in the response).

##### `start`
- *non-negative integer;* result at which to start at *(default: 0)*
- **NOTE:** the server *should* honor the `start` parameter
- **TIP:** increment `start` by `limit` to paginate through many results

##### `query`
- *MongoDB query;* use to specify file-entry fields/ranges; forwarded to MongoDB daemon

##### `keys`
- *a `|`-delimited string-list of keys;* defines what fields to include in result(s)
- ex: `"foo|bar|baz"`
- different routes/methods define differing defaults
- **NOTE:** there is no performance hit for including more fields
- *see [`all-keys`](#shortcut-parameter-all-keys)*

##### `max_time_ms`
- *non-negative integer OR `None`;* timeout to kill long queries in MILLISECONDS
- overrides the default timeout of 600000 ms (10 minutes)
- `None` indicates no timeout (this can hang the server -- you have been warned)

##### Shortcut Parameters: `logical-name-regex`, `logical_name`, `directory`, `filename`
*In decreasing order of precedence...*
- `logical-name-regex`
  - query by regex pattern (at your own risk... performance-wise)
  - equivalent to: `query: {"logical_name": {"$regex": p}}`

- `logical_name`
  - equivalent to: `query["logical_name"]`

- `directory`
  - query by absolute directory filepath
  - equivalent to: `query: {"logical_name": {"$regex": "^/your/path/.*"}}`
  - **NOTE:** a trailing-`/` will be inserted if you don't provide one
  - **TIP:** use in conjunction with `filename` (ie: `/root/dirs/.../filename`)

- `filename`
  - query by filename (no parent-directory path needed)
  - equivalent to: `query: {"logical_name": {"$regex": ".*/your-file$"}}`
  - **NOTE:** a leading-`/` will be inserted if you don't provide one
  - **TIP:** use in conjunction with `directory` (ie: `/root/dirs/.../filename`)

##### Shortcut Parameter: `run_number`
- equivalent to: `query["run.run_number"]`


##### Shortcut Parameter: `dataset`
- equivalent to: `query["iceprod.dataset"]`


##### Shortcut Parameter: `event_id`
- equivalent to: `query: {"run.first_event":{"$lte": e}, "run.last_event":{"$gte": e}}`


##### Shortcut Parameter: `processing_level`
- equivalent to: `query["processing_level"]`


##### Shortcut Parameter: `season`
- equivalent to: `query["offline_processing_metadata.season"]`

##### Shortcut Parameter: `all-keys`
- *boolean (`True`/`"True"`/`"true"`/`1`);* include *all fields* in result(s)
- **NOTE:** there is no performance hit for including more fields
- **TIP:** this is preferred over querying `/api/files`, grabbing the uuid, then querying `/api/files/{uuid}`



## Development

### Establishing a development environment
Follow these steps to get a development environment for the File Catalog:

    cd ~/projects
    git clone git@github.com:WIPACrepo/file_catalog.git
    cd file_catalog
    ./setupenv.sh

### MongoDB Instance for Testing
This command will spin up a disposable MongoDB instance using Docker:

    docker run \
        --detach \
        --name test-mongo \
        --network=host \
        --rm \
        circleci/mongo:latest-ram

### Building a Docker container
The following commands will create a Docker container for the file-catalog:

    docker build -t file-catalog:{version} -f Dockerfile .
    docker image tag file-catalog:{version} file-catalog:latest

Where {version} is found in file_catalog/__init__py; e.g.:

    __version__ = '1.2.0'       # For {version} use: 1.2.0

### Pushing Docker containers to local registry in Kubernetes
Here are some commands to get the Docker container pushed to our Docker
register in our Kubernetes cluster:

    kubectl -n kube-system port-forward $(kubectl get pods --namespace kube-system -l "app=docker-registry,release=docker-registry" -o jsonpath="{.items[0].metadata.name}") 5000:5000 &
    docker tag file-catalog:{version} localhost:5000/file-catalog:{version}
    docker push localhost:5000/file-catalog:{version}
