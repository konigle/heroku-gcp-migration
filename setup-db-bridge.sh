#!/bin/bash

# install postgres-14 along with development headers
sudo apt update
sudo apt-cache search postgresql
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt -y update
sudo apt -y install postgresql-14 postgresql-server-dev-14


# install git and cloud proxy
sudo apt -y install git python-pip tmux
curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.0.0/cloud-sql-proxy.linux.amd64
chmod +x cloud-sql-proxy


# londiste setup
sudo apt-cache madison londiste
sudo apt-cache madison pgq
sudo apt-get -y install pgqd python3-londiste