# file_catalog
Store file metadata information in a file catalog

## Interface

The primary interface is an HTTP server. TLS and other security
hardening mechanisms are handled by a reverse proxy server as
for normal web applications.

## Browser

Requests to the main url `/` are browsable like a standard website.

## RPC API

Requests with urls of the form `/api/RESOURCE/METHOD` can access the
RPC API. Arguments can be passed via GET or POST. Responses are
in JSON objects, which will contain a property `ok` to indicate
success or failure. An additional property `error` will be present
on failure to indicate the type of error. All HTTP responses should
return a status code of 200; other statuses are non-application
errors.

Note the [CRUD semantics](https://en.wikipedia.org/wiki/Create,_read,_update_and_delete)
of directories and files.

### Directories

Directories are "virtual", only present in the catalog.
They may mirror real directory structures, but are not required to.

* `/api/directories/list`: list directory contents

  Arguments:
  
  * path: path of directory

  Output:
  
  ```json
  {"status": "ok",
   "directories": ["PATH1", "PATH2"],
   "files": ["PATH3", "PATH4"]
  }
  ```

* `/api/directories/create`: create a new directory

  Arguments:
  
  * path: path of new directory
  
  * metadata: JSON-encoded metadata (optional)

* `/api/directories/read`: read metadata for a directory

  Arguments:

  * path: path of directory
  
  * metadata: JSON-encoded metadata

  Output:
  
  ```json
  {"status": "ok",
   "directory": {"path":"XXX",
                 "metadata": {}
                }
  }
  ```

* `/api/directories/update`: update metadata for a directory

  Arguments:

  * path: path of directory
  
  * metadata: JSON-encoded metadata

* `/api/directories/delete`: delete a directory and its contents

  Arguments:
  
  * path: path of directory

### Files

Files are stored as metadata.

* `/api/files/create`: create a new file or add a replica

  If a file exists and the checksum is the same, a replica
  is added. If the checksum is different, either the old
  file is replaced (overwrite: true) or an error is returned.

  Arguments:
  
  * path: path of file
  
  * replica: physical location of file
  
  * checksum: checksum of file
  
  * overwrite: overwrite a previous file (default false)
  
  * metadata: JSON-encoded metadata (optional)

* `/api/files/read`: read file information and metadata

  Arguments:
  
  * path: path of file
  
  Output:
  
  ```json
  {"status": "ok",
   "file": {"path":"XXX",
            "replicas": [],
            "checksum": "XXX"
            "metadata": {},
           }
  }
  ```

* `/api/files/update`: update file metadata

  Arguments:
  
  * path: path of file
  
  * metadata: JSON-encoded metadata

* `/api/files/delete`: delete a file

  This does not physically delete the file from the repliacs.
  Only the metadata in the catalog is removed.

  Arguments:
  
  * path: path of file

  * replica
