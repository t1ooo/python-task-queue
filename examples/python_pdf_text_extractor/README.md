PDF text extraction service.

A modified version of my service that originally used Celery as a task queue.


```sh
# start tasks worker
make serve-tasks

# start the FastAPI server:
make serve

# run tests
make test
```