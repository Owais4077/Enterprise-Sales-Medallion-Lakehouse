.PHONY: install test lint format check docker-test
install:
	pip install -r requirements-dev.txt
test:
	pytest
lint:
	ruff check .
format:
	black . && ruff check --fix .
check: lint
	black --check .
	pytest
docker-test:
	docker compose run --rm app pytest
