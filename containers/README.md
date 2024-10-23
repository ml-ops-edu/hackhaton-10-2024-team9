# DOCKER COMPOSE

## Description

Run a small Trino cluster using docker compose, with a remote Hive metastore, Mysql and Minio s3.
This config should be used primarily for testing purposes, such as testing or troubleshooting a Trino-Hive-MySQL-Metastore-S3 configuration.

### Prerequisite:

- A Docker engine (tested on v24) and client (tested on v26) with docker compose (e.g. colima).

### Deploy the docker compose stack:

- Start the docker engine. We recommend 8g min of memory, e.g. `colima start --memory 8`
- On the host, prepare the environment:
  - Run the command: `docker run -v ${PWD}:/initialize -it --rm eclipse-temurin:21-jdk /initialize/init.sh`
      - Under linux or macos with bash or a bash-compatible shell (zsh) you can run directly: `./init.sh`
      - Under other operating system you may need to replace ${PWD} with the full path to the current folder 
      - Run _./init.sh -f_ to clear an existing configuration and start from scratch
  - This will generate the certificates, the mysql and minio credentials, and config files from templates:
      - _.workdir/config/_
      - _.workdir/certs/_
      - _minio.env_
      - _mysql.env_
      - _trino.env_
      - _.env_
- If using an external S3 store, edit _minio.env_ to change the s3 endpoint, the bucket and your s3 key and secret and repeat the above command.
- On the host, run: `docker compose -p trinohack up -d`
  - As illustrated in the figure below, this command will (assuming -p _trinohack_, otherwise replace _trinohack_ accordingly):
    - Set up a docker bridge network _trinohack_trinocluster_
    - Set up docker named volumes _trinohack_mysqldb_ and _trinohack_minio_s3_
    - Start a mysql db service _trinohack-mysql-1_ for the hive metastore on _mysql:3306_
    - Start a minio s3 service _trinohack-minio-1_ for the S3 store on _minio:9000_ and publish as _https://localhost:9000_ (S3) and _https://localhost:9001_ (console)
    - Start a hive metastore service _trinohack-metastore-1_ on _thrift://metastore:9083_
    - Start a trino coordinator service _trinohack-trino-coordinator-1_ on _trino-coordinator:8080_, and publish as _http://localhost:8080_
    - Start one trino worker _trinohack-trino-worker-1_, which will report to the trino coordinator
- If using the local minio S3 service, use browser to log to the admin console at _https://localhost:9001_ with the credentials _MINIO_ROOT_USER_ and _MINIO_ROOT_PASSWORD_ from _minio.env_ - since we use a self-signed certificate, you are asked to accept the risks - and:
    - Create a user, using values of _S3_KEY_ and _S3_SECRET_ from _minio.env_ as user name and password respectively. Assign _readwrite_ access policy to the user.
    - Create the bucket using the value of _S3_BUCKET_ from _minio.env_ (normally _hackathon_)
    - Note: the bucket and user are automatically created when running the [test suite](../test/)

![docker stack](../docs/figs/docker-compose-doc.svg)

### Test
- After deploying a trino cluster using `docker compose -p trinohack up -d`
- Verify the status: `docker compose -p trinohack ps -a`, or `docker compose -p trinohack top`
- Scale up/down the trino cluster as desired, e.g. for 3 workers: `docker compose --env-file .env -p trinohack scale trino-worker=3`
- Open _http://localhost:8080_ in a browser to access the trino console (any user name, no password, replace 8080 by value of _TRINO_URL_PORT_ if set) 
- Run the [test suite](../test/README.md), or
- Start a trino client: `docker exec -it trinohack-trino-coordinator-1 trino`, and send Trino SQL commands, e.g. (rename the bucket name _hackathon_ as needed):

```
show session;
show catalogs;
create schema if not exists hive.db ;
show schemas from hive;
use hive.db ;
create table bands(name varchar, since integer) with(external_location='s3a://hackathon/hive/data/bands', format='PARQUET');
insert into bands values('foo', 1997);
insert into bands values('bar', 2008);
select * from bands order by since;
set session hive.non_transactional_optimize_enabled=true;
alter table bands execute optimize;
show schemas from iceberg;
use iceberg.db ;
create table stores (name varchar, city varchar) with(location='s3a://hackathon/hive/data/stores', format='PARQUET');
insert into stores values('shop1', 'city1'),('shop2', 'city2'),('shop3','city3');
alter table stores execute optimize;
alter table stores execute expire_snapshots(retention_threshold => '1d');
select * from stores;
select * from bands;
show schemas from s3;
use s3.db;
create table products (name varchar, stock int) with(auto_purge=true, external_location='s3a://hackathon/hive/data/products', format='PARQUET');
insert into products values('fork',10),('knive',0),('spoon',3),('glass',8);
select * from bands;
select * from products;
select * from stores;
drop table if exists iceberg.db.stores;
drop table if exists hive.db.bands;
drop table if exists s3.db.products;
drop schema if exists iceberg.db cascade;
drop schema if exists hive.db cascade;
drop schema if exists s3.db cascade;
```

### Troubleshooting

- If a container keeps restarting:
  - Modify the docker-compose yaml file and set _services.<service-id>.deploy.restart_policy.condition: no_ to turn off the restart policy of the misbehaving container.
  - Check the logs: _docker logs CONTAINER_NAME_
  - Check OOMKilled status (oom killer): `docker inspect container_name -f '{{ json .State.OOMKilled }}'`
    - If _true_:
      - Increase the docker engine memory (e.g. colima start --memory 8)
      - Adjust the _config/trino/\*/jvm.config\*_ files
      - Adjust the docker-compose's resource limits and _JAVA_TOOLS_OPTS_'s _XX:MaxRAM_
- If you get the error _Class org.apache.hadoop.fs.s3a.S3AFileSystem not found_, or similar:
  - Verify that _hadoop-aws-3.\*.\*.jar_ is present in hive metastore's _/opt/hive/lib_.
- If you get the error _Class com.amazon.auth.AWSCredentials.class not found_, or similar:
  - Verify that _aws-java-sdk-bundle-1.\*.\*jar_ is present in hive metastore's _/opt/hive/lib_.
- Other s3 related errors:
  - Edit _minio.env_ and make sure your s3 key and secret are there, that the correct endpoint is set.
  - Verify that the name of the bucket and the path are correct, and that you have read and write permissions (use aws or s3cmd).
- Metastore fails to start due to permission denied on mysql:
  - You may be starting a mysql service with a new _mysql.env_ settings on an existing _trinohack_mysqldb_ docker volume.
   You will be warned about that if you regenerate mysql.env with `./init.sh -f`.
   Rollback to the _mysql.env_ used to create the volume, or delete the volume with `docker volume rm trinohack_mysqldb`.
- The trino-coordinator admin web console is published on port 8080 and accessible from a browser in the host environment.
  If this conflicts with another app on the host, set _TRINO_URL_PORT_ to an available port before running docker compose up.
- The minio S3 http rest API and admin web console are published on port 9000 and 9001 respectively.
  If this conflicts with other apps on the host, set _MINIO_ENTRYPOINT_ and/or _MINIO_URL_PORT_ respectively to available ports before running docker compose up.
- The mysql and the metastore ports are not published by default:
  - To make them available to the host computer configure the corresponding _services.[mysql,metastore].ports_ in the docker compose file.

### Cleanup

- Shallow (preserve metastore and tables): `docker compose -p trinohack down`
- Deep:
  - `docker volume ls`, and delete minio and mysql volumes, e.g. `docker volume rm trinohack_mysqldb trinohack_minios3`
  - rm -rf .workdir

### Docker Cheat-Sheet

Below are a list of the docker commands that may be handy for this project

- `docker compose -p NAME up -d`: start a docker containers stack with given name, a docker-compose.yml file must be present
- `docker compose -p NAME ps`: show status of containers in the named container stack
- `docker compose -p NAME scale SERVICE=N`: up/down scale a service (must be mode replicated in docker compose)
- `docker compose -p NAME down`: terminate the named container stack (not all resources are deleted, see docker volume)
- `docker logs NAME`: show the logs of the named container
- `docker ps -a`: show all containers (both running and stopped)
- `docker exec -it NAME /bin/sh`: start a shell in a running container (replace /bin/sh by any other command, run `docker exec -it --user root ...` for admin role)
- `docker images -a`: list all images
- `docker RESOURCE_TYPE inspect NAME(s)`: inspect a resource of given type (_image_, _container_, _volume_, _network_, ...)
- `docker volume ls`: list the volumes used by docker
- `docker volume rm NAME(s)`: delete the volumes
- `docker volume prune --all`: delete all the volumes not used by a container
- `docker network ls`: list the networks
- `docker help` and `docker COMMAND --help`: get help
- `docker RESOURCE_TYPE ls`: list resources of given type (_image_, _container_, _volume_, _network_, ...), some have shortcuts (e.g. images, and ps).
- `docker RESOURCE_TYPE rm NAME(s)`: remove named resources of given type (_image_, _container_, _volume_, _network_, ...)
- `docker RESOURCE_TYPE prune`: safely remove unused or dangling anonymous resources of given type (_image_, _container_, _volume_, _network_, ...), use `--all` to remove _all_ unused resources of the given type.

### Notes
- The stock Apache Hive 4.0.1 image can be used for the metastore with the following caveats:
  - 3 versions of the guava jar are included in Hadoop, Hive, and Tez. Online guides recommend replacing them all with the newest version. However, preliminary tests suggest that this is not required (yet).
  - The hadoop-aws-3.3.6.jar and the aws-java-sdk-bundle-1.12.367.jar must be copied from /opt/hadoop/share/hadoop/tools/lib/ to /opt/hive/lib (it will fail without them)
  - Run docker compose up with _HIVE_IMAGE=image:tag_ to use a custom hive metastore image.
- Other alternatives to mysql (or mariadb) for the hive metastore are Derby (default and not recommended in production), and Postgres.
- You can deploy multiple trino docker stacks in isolation, use different values for -p, _MINIO_URL_PORT_, _MINIO_ENTRYPOINT_, and _TRINO_URL_PORT_.
- This stack is preconfigured with 3 trino catalogs:
  - hive: apache hive with legacy s3.
  - s3: apache hive with native s3.
  - iceberg: apache iceberg with native s3.
- Setting _hive.metastore.warehouse.dir_ in _hive-site.xml_ and trino catalogs to an s3 path has no effect. As a result, trino/hive managed tables are not supported. This is maybe for the better, and unless there is a demand for it all tables must be explicitely created with an s3 location.

