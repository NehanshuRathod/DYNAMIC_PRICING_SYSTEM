.PHONY: help install dev seed train test test-all audit lint load-test simulate docker-up docker-down clean

help:
	@echo ""
	@echo "  Dynamic Pricing Engine"
	@echo ""
	@echo "  Setup"
	@echo "    make install       Install all Python dependencies"
	@echo "    make train         Train XGBoost GBR + export ONNX"
	@echo "    make seed          Seed Redis with static fake data"
	@echo "    make simulate      Flink simulation — dynamic Redis population"
	@echo ""
	@echo "  Run"
	@echo "    make dev           DPE service locally"
	@echo "    make dev-event-api Event ingestion API locally"
	@echo "    make docker-up     Full stack (Redis + Kafka + DPE)"
	@echo "    make docker-down   Stop all containers"
	@echo ""
	@echo "  Test"
	@echo "    make test          Core DPE unit tests"
	@echo "    make test-all      All tests including extended"
	@echo "    make load-test     p99 latency load test"
	@echo "    make audit         Fairlearn fairness audit"
	@echo "    make lint          Ruff linter"
	@echo ""

install:
	pip install -r requirements.txt

train:
	mkdir -p artifacts
	python scripts/train_model.py --output artifacts/pricing_model.onnx --samples 50000

seed:
	python scripts/seed_redis.py --users 200 --skus 100

simulate:
	python flink_pipeline/simulate.py --mode synthetic --users 200 --skus 100 --events 5000

dev:
	uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload --loop asyncio

dev-event-api:
	uvicorn event_ingestion_api.app.main:app --host 0.0.0.0 --port 8000 --reload

test:
	pytest tests/test_dpe.py -v --tb=short

test-all:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ -v --cov=app --cov=flink_pipeline --cov-report=term-missing

load-test:
	python scripts/load_test.py --rps 50 --duration 20

audit:
	python scripts/fairness_audit.py

lint:
	ruff check app/ scripts/ tests/ flink_pipeline/ event_ingestion_api/ --fix

docker-up:
	docker compose up --build -d
	@sleep 8
	@$(MAKE) seed
	@echo "DPE ready at http://localhost:8001/docs"

docker-down:
	docker compose down -v

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .coverage htmlcov artifacts/*.onnx
	@echo "Cleaned"
