# MySQL Next
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
Setup is relatively straightforward! For each table or view that you would like to replicate, you just need to add two
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