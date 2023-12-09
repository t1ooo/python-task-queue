.PHONY: serve-tasks
serve-tasks:
	poetry run python -m python_task_queue app.tasks_backend:task_queue


.PHONY: serve
serve:
	poetry run uvicorn app.main:app --reload


.PHONY: test
test:
	poetry run pytest --verbose --capture=no