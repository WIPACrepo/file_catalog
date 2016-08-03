# file_catalog
Store file metadata information in a file catalog

## Interface

The primary interface is an HTTP server. TLS and other security
hardening mechanisms are handled by a reverse proxy server as
for normal web applications.

## Browser

Requests to the main url `/` are browsable like a standard website.

## REST API

Requests with urls of the form `/catalog/*` can access the
REST API.

### Directory listings

Directories are "virtual", only present in the catalog.
They may mirror real directory structures, but are not required to.

GET: Read directory listing

POST: Create directory (fail if present)

DELETE: Delete directory and contents

#### Example

`POST /catalog/sim/dataset_1234?isfile=false {metadata}`

Creates a new directory for dataset 1234, with optional metadata supplied
 in the parameters.

### Files

Files are stored as metadata.

GET: Read file metadata

POST: Create file (fail if present)

PUT: Create (overwrite) file

DELETE: Delete file

#### Example

`POST /catalog/sim/dataset_1234/000001 {metadata}`

Create a new file named 000001 in dataset 1234, with optional metadata
supplied in the parameters.

`PUT /catalog/sim/dataset_1234/000001 {metadata}`

Overwrite the file named 000001 in dataset 1234, with optional metadata

`GET /catalog/sim/dataset_1234/000001`

Return the metadata in json format.

### File replicas

Files can be stored in more than one location. The replica information is
a special part of the file metadata that needs additional specification.

When creating / overwriting a file, it will only contain a single replica.
This interface is for controlling additional replicas.

GET: Read list of replicas

POST: Add a replica

PUT: Overwrite a replica

DELETE: Delete a replica. If this is the last replica, delete the file.

#### Example

`POST /catalog/sim/dataset_1234/000001/replicas`

Add a replica.

`DELETE /catalog/sim/dataset_1234/000001/replicas/1`

Delete replica 1.

