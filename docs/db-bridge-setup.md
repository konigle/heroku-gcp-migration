# Setup Compute Engine For Database Replication

We use a dedicated VM instance on which Londiste replication agent will run. Below are the steps to set up this instance.

1. Create a VM instance in the same region as the database instance
    - Choose Ubuntu image for the boot disk
    - 10 GB disk space is enough
    - You can create a separate service account with `Cloud SQL Client` access scope and associate with this VM instance.
    - Make sure the instance is dedicated instance and keep at least 2 GB memory. We don't want the agent to run out of memory. We went with `e2-standard-2` instance.
2. Once the VM is ready, copy this full repo the VM instance. You can do git clone or clone locally and do scp.
```shell
gcloud compute scp --recurse --zone=<zone> heroku-gcp-migration <vm instance id>:~
```

3. SSH into the VM to get into the instance
```shell
gcloud compute ssh --project=<project_id> --zone=<zone> <instance_id>
```

4. Run the below script to install basic dependencies on the VM instance.
```shell
cd heroku-gcp-migration
./setup-db-bridge.sh
```

5. Run the below commands to set up Londiste
```shell
./setup-londiste.sh
```
This should finish without any errors.

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

Once these tools are set up, 

