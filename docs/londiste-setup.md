# Londiste Replication Setup

Londiste replication is based on PostgreSQL triggers. Hence, we need to set up these triggers
on and functions on the source database (Heroku) and the target (Cloud SQL) database.

The commands below require you to have database credentials of your source (Heroku) and target(Cloud SQL) databases.



1. Run the below command on the source(Heroku) database from the `londiste` directory.
```shell
cd londiste
psql postgresql://<username>:<password>@<host>:5432/<database>  -f init_noext.sql

```
This command will setup replication queues and triggers on the source database. It also creates several functions.

When run on the Heroku database, the above command will result on lot of errors related to postgres
role creation. This is due to the fact that Heroku will not allow role creation (which is sad if you are using dedicated instance :().
You can ignore these errors.

2. Run the below command on the target(Cloud SQL) database from the `londiste` directory as well.
```shell
psql postgresql://<username>:<password>@<host>:5432/<database> --single-transaction -f init_noext.sql

```

Note that we are running the script in a single transaction. This command is not supposed to result in any errors. If it does, then
changes are not committed by the database. The command will install pgq and londiste on the target database.

Also, you need to be running cloud proxy on the VM instance to connect to the target database. Run the cloud proxy in a tmux session so that it will be still running when you come out of SSH session.

With these steps, we have installed Londiste and PGQ on both source and target databases. It is time to start the
replication services.

## Running PGQ demon
1. Make changes to the `pgqd.ini` config file to specify the database, user credentials and list of databases to monitor for replication
2. Start the PGQ demon
```shell
pgqd -d pgqd.ini
```


## Create Londiste Source Node

1. Make changes to `source.ini` config file to update database credentials
2. Create the root node for replication. Root node will be your Heroku instance
```shell
londiste source.ini create-root primary "dbname=<heroku db name> port=5432 host=<heroku db host> password=<heroku db password> user=<heroku db user>"

```
3.Start the worker demon for the root node.
```shell
londiste -d source.ini worker
```

## Create Londiste Leaf / target Node

1. Make changes to `target.ini` to add database credentials
2. Add the leaf node. This will be your Cloud SQL instance.
```shell
londiste target.ini create-leaf subscriber "dbname=<dbname> port=5430 user=<username> password=<password> host=localhost" --provider="dbname=<heroku db name> port=5432 host=<heroku db host> password=<heroku db password> user=<heroku db user>"
```
3. Start the leaf node worker
```shell
londiste -d target.ini worker
```

Now we are ready to add the tables for replication. However, there is one more step to carry out
before we add the tables. 

When we add the tables, Londiste will start the initial copy of the table data from source to target. If you have
any foreign keys on the tables, the initial copy will fail. Hence, we need to first drop the
foreign keys on the target Cloud SQL tables before we add tables for replication.

In order to do that, execute the below commands from the `sql` directory on the **target** database.

1. Collect FK definitions into a separate table.
```shell
cd sql
psql postgresql://<username>:<password>@<host>:5432/<database>  -f collect_fks.sql

```
This will create new table in a separate schema and gather all foreign key definitions present on tables. This information
is needed when we want to restore the foreign key constraints once initial copy is done.

2. Drop the foreign keys.
```shell
psql postgresql://<username>:<password>@<host>:5432/<database> -f drop_fks.sql
```

Now we are ready to add the tables for replication! The tables and sequences need to be added to both source and
target nodes.

1. Add the sequences first.
```shell
londiste source.ini add-seq --all
londiste target.ini add-seq --all --no-triggers
```

2. Add the tables to replicate
```shell
londiste source.ini add-table --all
londiste target.ini add-table --all --no-triggers
```
It is important that we exclude triggers on the leaf node. Without this the `truncate` command
on the tables will fail on the target node and initial copy will fail.

Soon after the tables are added to the target node, londiste will start a number of parallel processes to perform
initial copy of the tables. You can specify the number of parallel copy processes in `target.ini` config file.

If you don't want any tables to be replicated, you can remove these.
```shell
londiste target.ini remove-table <table name>
londiste source.ini remove-table <table name>
```

## Check replication status

In order to check status of the replication, run the following command
```shell
londiste target.ini status
```
Check the status of each table by running
```shell
londiste target.ini tables
```
Once all the source and target tables are in sync, the above command will show `ok` status for all the tables.
When the initial copy is in progress, you can see the pid files created by the copy processes in
the `run` directory.

## Restore Dropped Foreign Keys

Once the initial copy is complete, you can restore the foreign keys. Run the below command on the target
database.
```shell
psql postgresql://<username>:<password>@<host>:5432/<database> -f restore_fks.sql

```

## Validate Foreign Key Constraints
Once the foreign key constraints are back in place, you can run the below command to validate the constraints
on the target database. Note that, if the tables are big, this command will take quite some time.

```shell
psql postgresql://<username>:<password>@<host>:5432/<database> -f validate_fks.sql

```

## Some Observations
- The initial copy could take several hours to days depending on the size of the tables.
- When the replication is set up, the Heroku database could experience increased load and slow things down