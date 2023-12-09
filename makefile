.PHONY: test
test:
	poetry run pytest -svvl --strict-markers -m "not redis and not peewee and not rabbitmq" tests/*

.PHONY: test-all
test-all:
	poetry run pytest -svvl --strict-markers tests/*
