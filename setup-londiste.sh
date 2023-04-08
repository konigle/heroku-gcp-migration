#!/bin/bash
mkdir -p londiste

cd londiste

git clone https://github.com/pgq/pgq.git
git clone https://github.com/pgq/pgq-node.git
git clone https://github.com/pgq/londiste-sql.git

cd pgq
git checkout v3.4.1
make
cd ..

cd pgq-node
git checkout v3.4.1
make
cd ..

cd londiste-sql
git checkout v3.4.1
make
cd ..

mkdir -p log
mkdir -p run
