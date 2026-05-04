.PHONY: run backfill migrate lint test docker-up docker-down clean

# ── Pipeline ──────────────────────────────────────────────────────────────────

run:
	python -m src.orchestrate.flows

backfill:
	python -m src.orchestrate.cli backfill --start $(START) --end $(END)

# ── Infrastructure ────────────────────────────────────────────────────────────

migrate:
	psql $(DATABASE_URL) -f db/migrations/001_init.sql

docker-up:
	docker-compose up -d postgres minio minio-init prefect-server prefect-worker
	@echo "Waiting for services to be healthy..."
	@sleep 10

docker-down:
	docker-compose down

docker-up-all:
	docker-compose up -d

# ── Quality ───────────────────────────────────────────────────────────────────

lint:
	ruff check src/ tests/
	black --check src/ tests/

fmt:
	black src/ tests/
	ruff check --fix src/ tests/

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

test-contract:
	pytest tests/contract/ -v

test-spark:
	pytest tests/spark/ -v

# ── Prefect deployment ────────────────────────────────────────────────────────

deploy:
	python src/orchestrate/schedules.py

# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf .ruff_cache .pytest_cache
