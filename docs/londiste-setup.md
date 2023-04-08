# Replication setup

Londiste replication is based on PostgreSQL triggers. Hence we need to set up these triggers
on and functions on the source database (Heroku) and the target (Cloud SQL) database. Follow the steps below


1. Run the below command on the source database from the `londiste` directory.
```shell
psql postgresql://<username>:<password>@<host>:5432/<database>  -f init_noext.sql

```

When run on the Heroku database, the above command will result on lot of errors related to postgres
role creation. This is due to the fact that Heroku will not allow role creation for the users.
You can ignore these errors.

2. Run the below command on the target database from the `londiste` directory as well.
```shell
psql postgresql://<username>:<password>@<host>:5432/<database> --single-transaction -f init_noext.sql

```

Note that we are running the script in a single transaction. This command is not supposed to result in any errors. If it does, then
changes will not take place.

Also, you need to be running cloud proxy on the VM instance to connect to the target database. Run the cloud proxy in a tmux session
so it will be still running when you come out of SSH session.

## Running PGQ demon
1. Make changes to the pgqd.ini config file to specify DB and user information and list of databases to monitor
2. Start the demon
```shell
pgqd -d pgqd.ini
```

## Setup and run Londiste source node

1. Create the root node for replication. Root node will be your Heroku instance
```shell
londiste source.ini create-root primary "dbname=<heroku db name> port=5432 host=<heroku db host> password=<heroku db password> user=<heroku db user>"

```

2. Start the worker demon for the root node. Note that you need to make changes to source.ini to add database info.
```shell
londiste -d source.ini worker
```

## Setup and run Londiste leaf / target node

1. Make changes to target.ini to add database info
2. Add the leaf node
```shell
londiste target.ini create-leaf subscriber "dbname=<dbname> port=5430 user=<username> password=<password> host=localhost" --provider="dbname=<heroku db name> port=5432 host=<heroku db host> password=<heroku db password> user=<heroku db user>"
```
3. Start the leaf node worker
```shell
londiste -d target.ini worker
```

Now we are ready to add the tables for replication. However, there is one more step to carry out
before we add the tables. 

When we add the tables, Londiste will start initial copy of the table data from source to target. If you have
any foreign keys on the tables, the initial copy will fail. Hence, we need to first drop the
foreign keys on the target CLoud SQL table before we start the copy.

In order to do that, execute the below commands from the `sql` directory.

1. Collect FK definitions into a separate table
```shell
psql postgresql://<username>:<password>@<host>:5432/<database>  -f collect_fks.sql

```

2. Drop the foreign keys for the time being
```shell
psql postgresql://<username>:<password>@<host>:5432/<database> -f drop_fks.sql
```

Now we are ready to add the tables for replication!

1. Add the sequences first
```shell
londiste source.ini add-seq --all
londiste target.ini add-seq --all --no-triggers
```

2. Add the tables to replicate
```shell
londiste source.ini add-table --all
londiste target.ini add-table --all --no-triggers
```
It is important that we exclude triggers on the leaf node. Without this the truncate command
on the tables will fail on the target node and initial copy will fail.

If you don't want any tables to be replicated, you can remove these.
```shell
londiste target.ini remove-table <table name>
```

## Check replication status
In order to check status of the replication, run the following command
```shell
londiste target.ini status
```

## Restore dropped FKs
```shell
psql postgresql://<username>:<password>@<host>:5432/<database> -f restore_fks.sql

```
