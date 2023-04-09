# Migrating Heroku Postgres to Cloud SQL
The overall objective is to migrate the infrastructure with minimum 
downtime. In order to achieve this objective, it is crucial to have
database replication outside of Heroku. Even though Postgres supports 
logical replication outside of Heroku, Heroku does not provide necessary 
replication privileges which is unfortunate. We will overcome this by 
setting up [Londiste](https://manpages.ubuntu.com/manpages/xenial/man1/londiste3.1.html) logical replication tool. There are very few guides
on correctly using Londiste. This guide should help you set up trigger based
logical replication on Heroku Postgres.

The overall process involves
1. Setting up Cloud SQL target instance that will be a logical replica for Heroku Postgres
2. Setting up a VM instance on GCP on which the Londiste replication services run
3. Setting up Londiste services

Let's go step by step

## Setup Cloud SQL instance
Google Cloud has detailed documentation on setting up Cloud SQL instance for
a project. Few things to keep in mind
- Allocate sufficient disk space that can at least fit the current Heroku database
- Choose the appropriate region before creating the instance. The zone selection can be done keeping below aspects in mind
  - Keep the database closer to the application
  - Keep the application closer to the majority of your customers
  - Refer [Google's Latency Matrix](https://lookerstudio.google.com/u/0/reporting/fc733b10-9744-4a72-a502-92290f608571) to check the avg. latency and throughput to make a data driven decision on which region to host the service.

Once you decide on the region, you can follow [Google's guide](https://cloud.google.com/sql/docs/postgres/create-instance) on setting up the Cloud SQL instance. 
- Create a database user to be used by your application and Londiste replication tool.
- Create a database to be used by the application - this is your production database.
Once SQL instance is up and running, you need to apply all the migrations so that the tables are created.

For a Django application, below commands apply.

1. Download Cloud SQL Proxy on your local computer
```shell
curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.0.0/cloud-sql-proxy.linux.amd64
```
2. Run the cloud proxy on the local computer where you have the application codebase.
```shell
./cloud-sql-proxy <project_id>:<region>:<instance_id> --port=5432
```
2. Run the migration from the project root
```shell
python manage.py migrate
```

We don't recommend any database schema changes when the data migration is in progress.
If schema changes are needed, then you need additional steps to let londiste know about these changes.
Refer to [Londiste documentation](https://manpages.ubuntu.com/manpages/xenial/man1/londiste3.1.html) if such schema changes are necessary.

Once the migrations are applied on the Cloud SQL database (target database for replication), it contains all the tables defined by your application.

Once the database is set up, it is time to create a VM instance on which the Londiste will run.

## Setup VM Instance for Londiste
We will be running Londiste services on a VM instance inside GCP. Follow the VM instance creation guide by Google Cloud while keeping following considerations in mind


- Create a VM instance in the same region as the database instance
    - Choose Ubuntu image for the boot disk
    - 10 GB disk space is enough
    - Allocate at least 2 vCPUs and 4 GB RAM for the VM instance. Londiste consists of multiple services required to be running and several processes required for initial copy of the database.
    - You can create a separate service account with `Cloud SQL Client` access scope and associate with this VM instance.
    - Make sure the instance is dedicated instance and keep at least 2 GB memory. We don't want the agent to run out of memory. We went with `e2-standard-2` instance.
- It is preferable to locate the VM instance closer the Heroku Postgres. This can speed up initial copy of the database if the database is large.

Once the VM is ready, 
1. SSH into the instance and clone this repo.

```shell
gcloud compute ssh --project=<project_id> --zone=<zone> <instance_id>

git clone https://github.com/konigle/heroku-gcp-migration.git
```
You can clone locally and do scp.
```shell
gcloud compute scp --recurse --zone=<zone> heroku-gcp-migration <vm instance id>:~
```

2. Run the below script to install basic dependencies on the VM instance.
```shell
cd heroku-gcp-migration
./setup-db-bridge.sh
```

3. Run the below command to set up Londiste
```shell
./setup-londiste.sh
```
This should finish without any errors. The SQL scripts by PGQ and Londiste are modified and copied under `londiste` directory to make them
compatible with Heroku Postgres.

After this setup, you will have below tools installed on the VM.
1. londiste - replication agent
```shell
which londiste
>> /usr/bin/londiste
   
londiste --version
>> londiste, Skytools version 3.8.1
```

2. pgqd
```shell
which pgqd
>> /usr/bin/pgqd

pgqd -V
>> pgqd version 3.5
```

Once these tools are installed, it is time to set up the database replication.
Setting up the Londiste tool for replication is lengthy process. This deserves a separate document to keep things clear.

Follow [Londiste Setup Guide](./londiste-setup.md) to set up the replication.
