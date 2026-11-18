# Shortcuts. Run `make help` to list them.
.PHONY: help install lint format test test-all run demo dashboard up down

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## install runtime + dev dependencies
	pip install -r requirements-dev.txt

lint: ## ruff lint + format check
	ruff check . && ruff format --check .

format: ## auto-format with ruff
	ruff format . && ruff check --fix .

test: ## unit tests (Postgres tests skip unless TEST_DATABASE_URL is set)
	pytest

test-all: ## everything, against a separate games_test DB in the compose Postgres (tests drop tables!)
	-docker compose exec db createdb -U games games_test
	TEST_DATABASE_URL=postgresql+psycopg://games:games@localhost:5432/games_test pytest

run: ## run the pipeline for today
	python -m pipeline.run

demo: ## fill 21 days of SYNTHETIC data and run the pipeline on each day
	python scripts/seed_demo_data.py --days 21 --run

dashboard: ## serve the dashboard on http://localhost:8000
	python -m http.server 8000 --directory dashboard

up: ## docker compose up (Postgres + pipeline + dashboard)
	docker compose up --build

down: ## stop containers
	docker compose down
