COMPOSE=docker compose

.PHONY: build test run up down logs producer clean minio postgres

build:
	$(COMPOSE) build

test:
	$(COMPOSE) run --rm tests

run:
	$(COMPOSE) up --build stream-pipeline

up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f stream-pipeline

producer:
	docker compose run --rm stream-pipeline \
		python3 producer/generate_events.py \
		--events 20 \
		--output data/input/events.jsonl

minio:
	$(COMPOSE) up -d minio

postgres:
	$(COMPOSE) --profile warehouse up -d postgres

clean:
	rm -rf data/output/*
	rm -rf data/checkpoint/*
	rm -rf data/bronze/*
	rm -rf data/silver/*
	rm -rf data/gold/*
	rm -rf .pytest_cache
	rm -rf src/__pycache__
	rm -rf tests/__pycache__