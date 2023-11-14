# MySQL Binlog CDC Extractor

[Keboola Connection](https://www.keboola.com/) component for MySQL databases log based replication.

This connector works with MySQL databases hosted on AWS RDS, Aurora MySQL, and standard non-hosted MySQL.

## Functionality notes

Log-based replication is a type of change data capture (CDC) where incremental changes made to a database are detected
by reading the binary logs (AKA binlogs in MySQL) to pick up only changes since the last execution of this pipeline.
More specifically, all INSERT, UPDATE, and DELETE statements are appropriately recorded for database change capture.
This replication style is actually the fastest method for identifying change (faster than key-based replication in
almost every case) and has the ability to capture hard deletes (so long as they are run as a DELETE, not a TRUNCATE or 
DROP statement), unlike key-based replication. 

Deleted records will be left with a "deletion marker", identified by a
timestamp for the time the record was deleted in the special `KBC_DELETED_AT` column.

### Schema Change handling

Unlike majority solutions on the market, this connector is capable of handling schema changes ADD/DROP COLUMN **without
the need to initiate full-sync**. The schema changes are handled in a following manner:

- **ADD column**
  - Such column is added to the destination table. Historic values will be empty (default not reflected).
  - The event will be logged in the resulting `SCHEMA_CHANGES` table as change type `ADD_COLUMN`
- **DROP column**
  - The column will remain in the destination table.
  - It's values will be NULL/EMPTY since the deletion.
  - `DROP_COLUMN` event will be emitted into the `SCHEMA_CHANGES` table.



### Important Limitations

- Only `INSERT`, `UPDATE`, and `DELETE`statements are collected. This means that any deletes that are results of `TRUNCATE` statement or `DROP` and `CREATE` statements will not be collected.
- On schema change (ADD COLUMN, ALTER) the default values **will not** be reflected to historical records. E.g. if you introduce new column, the replication will continue, but the all historical records prior the column addition will be empty.
- ALTER statements changing datatypes will not be reflected, the original value will be kept.
- If `Native Datatypes` are turned on in the Keboola project, ALTER statement changes may cause unexpected errors.




## MySQL Setup

### Enable row-level log based replication

In order for log-based replication to work, you must have enabled [row-based binary logging](https://dev.mysql.com/doc/refman/8.0/en/binary-log-setting.html)
on your MySQL instance/cluster. If you are using Aurora MySQL you will need to use a
[database cluster parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_WorkingWithParamGroups.html)
with parameter '`binlog_format`' set to '`ROW`'.

By default, MySQL removes these binary logs as soon as possible. However, in order to read them effectively for
replication, we want to extend this retention period. You should set this to anywhere between 1 and 7 days. 

For example,  to set `3 day` retention period, run the following command:

```sql
call mysql.rds_set_configuration('binlog retention hours, 72);
```

The above only needs to be set up during initial setup for replication. You can also run
`call mysql.rds_show_configuration;` to see your existing binlog retention hours value, if any.

### User privileges 

The connector requires db user with privileges replication client privileges and read-only privileges to specified tables. 

Example SQL script to create the user:

```sql
create user keboola@'%' identified by '{insert strong password here}';
grant replication client, replication slave, select ON *.* TO keboola@'%';
```
If you follow the above, you will use Username 'keboola' during configuration, and the password you set for Password.

## Configuration 

### Connection and SSH Tunnel

For additional security you might need to whitelist Keboola IPs (see [here](https://help.keboola.com/components/ip-addresses)).

Fill in the database connection details:

- Hostname - url or hostname of your DB instance. In case you are usng SSH Tunnel, this should be the local network address.
- Port
- User
- Password


You may opt to connect through **SSH Tunnel**.  To do so, so select the `SSH Tunnel` option in the `Authorization` section and provide following parameters:

- SSH host IP
  - Note that this may be different from the actual MySQL Instance address. When using the tunnel the MySQL host is the address in the local network where your SSH tunnel resides, **NOT** the outside world IP
- Port - usually 22, but it is advised to change it to non-standard port 
- SSH user
- RSA SSH Private Key
  - The private key of the SSH user. Make sure that the public counterpart is properly added in the servers `ssh_keys`

#### Generating SSH Key pair

**Generate using UI**

You can let the component generate the SSH key pair for you. Click the `GENERATE SSH KEY PAIR` button to display the private and public key pair.

Make note of both, as they will be lost after you close the popup. Insert the private key to related configuration field and add the public key to ssh server's `ssh_keys`


**Generate key manually**

To generate SSH keypair you can use following bash command:

```shell
#!/bin/bash

# Generate the RSA key pair
ssh-keygen -t rsa -b 2048 -f /FOLDER_PATH/id_rsa -N ""
```
The generated private key will be saved in the `~/FOLDER_PATH/id_rsa` file, and the corresponding public key will be saved in the `~/FOLDER_PATH/id_rsa.pub` file.

### Advanced Options

- **Max Connection Time** - Optional parameter which sets `@@session.max_execution_time` to desired integer value. Use this when
  initial full syncs are failing due to the large size of the tables.
- **Show Binary Log Method** - Special parameter to force the connector to get the `SHOW BINLOG` result from a http
  endpoint instead.
  - This is a special edge-case option, in some cases our customers experienced very slow responses on Aurora databases of certain version
    when running `SHOW BINLOG` command. For this purpose they opted to expose the cached `SHOW BINLOG` result as an API endpoint.

### Sync Options

#### Replication Mode

- **Standard**
  - Performs full sync of the tables on the first run and then continues from the binlog.
- **Snapshot Only**
  - Performs full sync always.

#### Binary data handler

Binary data in most cases cannot be converted between databases 1:1. As our components result in CSV files as intermediate step, 
it is necessary to select Binary data handling strategy:

- **plain** - data in decoded from binary to string using Python's bytes.decode() method,
- **hex** - data is converted to hex representation of binary string using bytes.hex() method,
- **base64** - data is converted to a base64 string, using base64.b64encode() method.


### Data Source

Select schemas(databases) and tables you wish to sync. 

### Destination

- **Load Type**
  - Incremental Load - Events in each batch will be deduplicated and only the latest events will be _upserted_ into the
    destination table based on primary key. Resulting in fully replicated table
  - Append - Each event (INSERT, UPDATE, DELETE) will be appended to the resulting table as a separate row. The user is
    responsible for replication in the downstream processes.
- **Output Bucket** - (Optional) The name of bucket in Keboola storage where the resulting tables are stored. Keboola
  will create a bucket for you if not specified. The name is without the stage prefix e.g. `cdc-input`, the bucket stage
  will always be `IN`.

## Output

**The connector currently outputs all tables and columns in UPPERCASE**. The table names are prefixed with the schema(database) name. 
e.g. The result tables will be stored as `SCHEMA_TABLENAME`

### System columns

The connector generates additional system columns

| Column Name          | Descriptions                                                 |
|----------------------|--------------------------------------------------------------|
| **KBC_SYNCED_AT**    | UTC Timestamp of sync start                                  |
| **KBC_DELETED_AT**   | Epoch Timestamp of row deletion, otherwise NULL              |
| **BINLOG_CHANGE_AT** | Epoch Timestamp of last row change                           |
| **BINLOG_READ_AT**   | Time when the change was read from binlog as Epoch Timestamp |

### SCHEMA_CHANGES table

The schema changes will be logged into table `SCHEMA_CHANGES`:


| Column Name     | Descriptions                                  |
|-----------------|-----------------------------------------------|
| **schema**      | name of the schema                            |
| **table**       | name of the affected table                    |
| **change_type** | Typ of change (`DROP_COLUMN`,`ADD_COLUMN`)    |
| **column_name** | Name of affected column                       |
| **query**       | Full query that resulted in the schema change |
| **timestamp**   | Epoch Timestamp of the event                  |



## Legacy configuration format

For each table or view that you would like to replicate, you just need to add two
options to the "metadata" section of that table or view. First, specify `"selected": true`. Next, choose the replication 
method by setting `"replication-method": "INCREMENTAL"`. Allowed values are `FULL_TABLE`, `INCREMENTAL` and `LOG_BASED`.

If you choose `INCREMENTAL`, you also must specify a replication key, the field that will be used to determine if a 
given row in that table has changed. You specify this with an additional parameter, such as 
`"replication-key": "updated_at"`. `LOG_BASED` is only allowed if the server is set up to support it, and the database 
object is a table, not a view.

For any replication method, once you have chosen "selected" to True at the table level for each table/view you want to 
include, set `"selected": false` for any column(s) that you want to exclude from the replication (i.e. sensitive info),
by default all columns are included for selected tables.

By default all tables and views are excluded to protect potentially sensitive data.
However, if you choose to include a table, all columns are included by default (for ease of adding new tables); any
columns you would like to exclude must be explicitly set as such by including `selected: false` on that column.

#### Pulling Existing Schema Definitions
The extractor has the ability to pull existing schema definitions. If you set the parameter discover_schema to True, the
extractor will pull databases(schemas), tables and fields. You can then choose which to include or exclude.

#### Running Historical Syncs
Upon your first execution, all tables must run an initial first full sync. However, you need to set the configuration
file to do so, it will automatically. Full historical syncs on a particular table will be necessary in the future
whenever you update the schema you are pulling from that table. A full database re-sync will ONLY be necessary if you do
not run a sync for a long period of time, beyond your current 'binlog retention hours' setting.
 


## Development
 
This example contains runnable container with simple unittest. For local testing it is useful to include `data` folder in the root
and use docker-compose commands to run the container or execute tests. 

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path:
```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, init the workspace and run the component with following command:

```
git clone https://bitbucket.org:kds_consulting_team/kbc-python-template.git my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```
docker-compose run --rm test
```

## Testing

The preset pipeline scripts contain sections allowing pushing testing image into the ECR repository and automatic 
testing in a dedicated project. These sections are by default commented out. 

**Running KBC tests on deploy step, before deployment**

Uncomment following section in the deployment step in `bitbucket-pipelines.yml` file:

```yaml
            # push test image to ECR - uncomment when initialised
            # - export REPOSITORY=`docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD -e KBC_DEVELOPERPORTAL_URL quay.io/keboola/developer-portal-cli-v2:latest ecr:get-repository $KBC_DEVELOPERPORTAL_VENDOR $KBC_DEVELOPERPORTAL_APP`
            # - docker tag $APP_IMAGE:latest $REPOSITORY:test
            # - eval $(docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD -e KBC_DEVELOPERPORTAL_URL quay.io/keboola/developer-portal-cli-v2:latest ecr:get-login $KBC_DEVELOPERPORTAL_VENDOR $KBC_DEVELOPERPORTAL_APP)
            # - docker push $REPOSITORY:test
            # - docker run --rm -e KBC_STORAGE_TOKEN quay.io/keboola/syrup-cli:latest run-job $KBC_DEVELOPERPORTAL_APP BASE_KBC_CONFIG test
            # - docker run --rm -e KBC_STORAGE_TOKEN quay.io/keboola/syrup-cli:latest run-job $KBC_DEVELOPERPORTAL_APP KBC_CONFIG_1 test
            - ./scripts/update_dev_portal_properties.sh
            - ./deploy.sh
```

Make sure that you have `KBC_STORAGE_TOKEN` env. variable set, containing appropriate storage token with access 
to your KBC project. Also make sure to create a functional testing configuration and replace the `BASE_KBC_CONFIG` placeholder with its id.

**Pushing testing image for manual KBC tests**

In some cases you may wish to execute a testing version of your component manually prior to publishing. For instance to test various
configurations on it. For that it may be convenient to push the `test` image on every push either to master, or any branch.

To achieve that simply uncomment appropriate sections in `bitbucket-pipelines.yml` file, either in master branch step or in `default` step.

```yaml
            # push test image to ecr - uncomment for testing before deployment
#            - echo 'Pushing test image to repo. [tag=test]'
#            - export REPOSITORY=`docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD -e KBC_DEVELOPERPORTAL_URL quay.io/keboola/developer-portal-cli-v2:latest ecr:get-repository $KBC_DEVELOPERPORTAL_VENDOR $KBC_DEVELOPERPORTAL_APP`
#            - docker tag $APP_IMAGE:latest $REPOSITORY:test
#            - eval $(docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD -e KBC_DEVELOPERPORTAL_URL quay.io/keboola/developer-portal-cli-v2:latest ecr:get-login $KBC_DEVELOPERPORTAL_VENDOR $KBC_DEVELOPERPORTAL_APP)
#            - docker push $REPOSITORY:test
```
 
 Once the build is finished, you may run such configuration in any KBC project as many times as you want by using [run-job](https://kebooladocker.docs.apiary.io/#reference/run/create-a-job-with-image/run-job) API call, using the `test` image tag.

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 