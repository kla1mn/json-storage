DIRS=json_storage tests

init:
	python3.14 -m venv .venv
	.venv/bin/pip install uv
	uv venv --clear --python 3.14
	uv sync --all-extras

HOST ?= 0.0.0.0
PORT ?= 8079

start:
	uv run granian --interface asgi --host $(HOST) --port $(PORT) json_storage.cmd.rest:app


start_taskiq:
	taskiq worker json_storage.cmd.taskiq_broker:taskiq_broker json_storage.tasks --log-level=DEBUG


format:
	ruff format $(DIRS)

fix:
	ruff check --fix $(DIRS)

lint:
	ruff check --fix $(DIRS)
	ruff format $(DIRS)
	mypy .

up:
	docker compose up -d

down:
	docker compose down
