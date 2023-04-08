# Heroku to GCP Migration
This repo contains set of scripts and instructions to systematically migrate a Django application to Google Cloud.
The migration of database (PostgreSQL) and cache are the main focus. 

## Source application hosted on Heroku
Django application with Celery for async tasks
- Web type dyno for frontend
- Worker dyno for celery beat and workers
- CloudAMQP for RabbitMQ broker required by Celery
- Memcachier - used for distributed caching
- Heroku Postgres

## Objectives
- Minimum DevOps requirement on the GCP just like Heroku
- Low downtime migration of the production infra

## Migration guides

- [Database (PostgreSQL)](./docs/db-migration.md)
- [Django application server]()
- [Async and periodic tasks]()
- [Cache]()


## References
- [Running Django app on Cloud Run](https://cloud.google.com/python/django/run) 
- [Londiste documentation](https://manpages.ubuntu.com/manpages/xenial/man1/londiste3.1.html)