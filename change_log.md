**0.4.17**
- Output bucket option working correctly

**0.4.16**
- Append mode will not deduplicate based on primary key

**0.4.15**
- Added option for append mode only, which speeds up write to storage.

**0.4.14**
- Reduced traceback
- Fixed JSON mappings of database objects

**0.4.13**
- Added option to specify input mapping tables with JSON
- Binlog events are now filtered for specified tables only. This drastically improves replication speed.

**0.4.10**

- YAML support for input mapping specification
- Proper conversion to strings to avoid float/numeric conversion issues

**0.4.3**

- Allows replication-method to be specified in any case (not just all caps)
- Removed pandas for full table downloads for performance improvement
- Fixed output of ints and floats issue with pandas full sync
- Added docs on filling out mappings file

**0.3.4**

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