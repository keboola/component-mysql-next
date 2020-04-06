**0.3.2**

- Grabs only latest binlog event per primary key in extraction since data is uploaded by PK
- Supports sending column metadata to Keboola
- Full table syncs are marked with incremental false
- Added support for set MySQL data type
- Columns are capitalized for Snowflake transforms ease of use
- Primary keys and tables are capitalized

**0.2.25**

- Stable full sync and log-based incremental sync, with performance improvements to come
- Adds ability to manually specify state and outputs state as file mapping
- Proper manifest table handling

**0.2.14**

- Beta full sync that runs in chunks, should be much quicker, manifest file needs fixes
- Log-based still handled at row level

**0.1.1**

- SSH tunnel compatibility and initial full sync option ready.
- Fix for data path when running in container.

**0.1.0**

- Defined core libraries.
- Proof of concept execution on single MySQL table for full and log-based replication.