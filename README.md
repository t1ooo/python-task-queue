Celery like task queue

## Features
+ put task in queue
+ check task status by id
+ get task by id
+ get task result by id
+ cancel task
+ gracefully shutdown
+ brokers
    + redis
    + rabbitmq
    + peewee (sqlite, mysql, postgresql and cockroachdb)
    + [TODO] filesystem
    + [TODO] mongodb
+ storages
    + redis
    + rabbit
    + peewee (sqlite, mysql, postgresql and cockroachdb)
    + [TODO] filesystem
    + [TODO] mongodb
+ collect failed tasks
+ worker options:
    + count
    + polling delays
+ task options
    + number or retries
    + delay between retries 
        + [TODO] retries with exponential backoff
    + timeout
    + ttl
    + result ttl
    - [TODO] priority
+ task types
    + simple
    + [TODO] async
    + [TODO] scheduled
    + [TODO] with dependencies
+ [TODO] remove result from backend by id
+ [TODO] limit queue size
+ [TODO] collect dead letters
+ [TODO] monitoring
+ [TODO] support multiple task files 
+ [TODO] automatic reload after changing the source code of tasks


## Setup
```sh
# install python 3.12
pyenv install 3.12

# set python version for the current project
poetry env use /full/path/to/python3.12

# install project dependencies
poetry install --only main

# install dev dependencies
poetry install --only dev

# install example dependencies
poetry install --only example

# run tests
make test

# run all tests (requires RabbitMQ server, Redis server, sqlite3)
make test-all
```

## Examples
+ A basic example from a single file
    + examples/basic 
+ A PDF text extraction service that uses this library for the background task of extracting text from PDF
    + examples/python_pdf_text_extractor 


Tags: python, celery, task queue, queue, threads, multiprocessing, rabbitmq, redis, peewee
