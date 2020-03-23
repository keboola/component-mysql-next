## MySQL Next
#### Summary
The open source database enables delivering high-performance and scalable web-based and embedded database applications.
This configuration works for MySQL databases hosted on AWS RDS, Aurora MySQL, and standard non-hosted MySQL. MySQL next
allows for choice of replication style at the table level, supporting log-based incremental replication.

#### Log-Based Replication for Change Data Capture
Log-based replication is a type of change data capture (CDC) where incremental changes made to a database are detected
by reading the binary logs (AKA binlogs in MySQL) to pick up only changes since the last execution of this pipeline.
More specifically, all INSERT, UPDATE, and DELETE statements are appropriately recorded for database change capture.
This replication style is actually the fastest method for identifying change (faster than key-based replication in
almost every case) and has the ability to capture hard deletes (so long as they are run as a DELETE, not a TRUNCATE or 
DROP statement), unlike key-based replication. Deleted records will be left with a "deletion marker", identified by a
timestamp for the time the record was deleted in the special _KBC_DELETED_TIME column.

Generally speaking, a full sync is run during the first execution with log-based replication. From there, markers are
recorded to essentially keep track of the max of each table where the work was left off. From there, log-based
replication truly kicks in. All row-based binary logs for append, update and deletion events are captured and written to
Keboola storage. We record the place we last left off among those binary log files, and continue from there.

*Note*: A full table replication must be run if schema changes are necessary, i.e. adding a new column.

#### Setting Up the Database Connection
In order to connect to MySQL, you will supply your host name or IP, port (usually 3306), username and password. The
username and password you specify will essentially act as a Keboola service account. To create this user, you will need
to run the following SQL command against your instance:
```sql
create user keboola@'%' identified by '{insert strong password here}';
grant replication client, replication slave, select ON *.* TO keboola@'%';
```
If you follow the above, you will use Username 'keboola' during configuration, and the password you set for Password.

If you are authenticating without using an SSH tunnel, you will need to whitelist Keboola IPs (see [here](https://help.keboola.com/components/ip-addresses)).
If you are connecting via an SSH tunnel, specify 'ssh_tunnel' as True and supply the necessary parameters for your SSH
connection: SSH host IP, port (usually 22), SSH user and public key for accessing the SSH host.

#### Setting up Binary Logging
In order for log-based replication to work, you must have enabled [row-based binary logging](https://dev.mysql.com/doc/refman/8.0/en/binary-log-setting.html)
on your MySQL instance/cluster. If you are using Aurora MySQL you will need to use a
[database cluster parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_WorkingWithParamGroups.html)
with parameter 'binlog_format' set to 'ROW'.

By default, MySQL removes these binary logs as soon as possible. However, in order to read them effectively for
replication, we want to extend this retention period. You should set this to anywhere between 1 and 7 days. For example,
if setting to a 3 day retention period, you would run the following:
```sql
call mysql.rds_set_configuration('binlog retention hours, 72);
```
The above only needs to be set up during initial setup for replication. You can also run
`call mysql.rds_show_configuration;` to see your existing binlog retention hours value, if any.

#### Setting Up the Configuration File
[TODO]

#### Pulling Existing Schema Definitions
The extractor has the ability to pull existing schema definitions. If you set the parameter discover_schema to True, the
extractor will pull databases(schemas), tables and fields. You can then choose which to include or exclude.

#### Running Historical Syncs
Upon your first execution, all tables must run an initial first full sync. However, you need to set the configuration
file to do so, it will automatically. Full historical syncs on a particular table will be necessary in the future
whenever you update the schema you are pulling from that table. A full database re-sync will ONLY be necessary if you do
not run a sync for a long period of time, beyond your current 'binlog retention hours' setting.