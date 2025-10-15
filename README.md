### file_scanner

this application scans a path and creates a table in a provided database to store:

* File Name
* Full path to the file
* File checksum
* File creation time
* File modified time
* File last accessed time

  The intention is to use this information to generate a report outlining duplicate files and probably unused files.

#### Usage:
./file-scanner --path <path-to-scan> --db-conn "postgresql://<user>:<password>@<host>:<port>/<database>"
