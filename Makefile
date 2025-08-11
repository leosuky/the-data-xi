.PHONY: init start stop shell migrate createsuperuser test lint

init:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt

start:
	docker-compose up -d postgres

stop:
	docker-compose down

shell:
	/bin/bash

migrate:
	python manage.py migrate

createsuperuser:
	python manage.py createsuperuser

test:
	pytest -q

lint:
	black --check . && flake8
