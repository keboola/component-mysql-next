## MySQL Next

#### Summary
The open source database enables delivering high-performance and scalable web-based and embedded database applications.
This configuration works for MySQL databases hosted on AWS RDS, Aurora MySQL, and standard non-hosted MySQL. MySQL next
allows for choice of replication style at the table level, supporting log-based incremental replication.

#### Detail
Log-based replication is a type of change data capture (CDC) where incremental changes made to a database are detected
by reading the binary logs (AKA binlogs in MySQL) to pick up only changes since the last execution of this pipeline.
This replication style is actually the fastest method for identifying change (faster than key-based replication in
almost every case) and has the ability to capture hard deletes.