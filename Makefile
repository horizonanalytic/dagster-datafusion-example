.PHONY: help build up down logs restart setup-localstack clean test

help:
	@echo "DataFusion + Dagster Example - Available Commands"
	@echo "=================================================="
	@echo ""
	@echo "  make build           Build Docker images"
	@echo "  make up              Start all services (Dagster + LocalStack + Postgres)"
	@echo "  make down            Stop all services"
	@echo "  make logs            Tail logs from all services"
	@echo "  make restart         Restart all services"
	@echo "  make setup-localstack Initialize LocalStack S3 buckets"
	@echo "  make clean           Remove all containers, volumes, and generated data"
	@echo "  make test            Run tests"
	@echo ""

build:
	@echo "Building Docker images..."
	docker-compose build

up:
	@echo "Starting services..."
	docker-compose up -d
	@echo ""
	@echo "Waiting for services to be ready..."
	@sleep 15
	@echo ""
	@echo "Setting up LocalStack S3..."
	@./setup_localstack.sh
	@echo ""
	@echo "=========================================="
	@echo "Dagster UI: http://localhost:3000"
	@echo "=========================================="

down:
	@echo "Stopping services..."
	docker-compose down

logs:
	docker-compose logs -f

restart: down up

setup-localstack:
	@./setup_localstack.sh

clean:
	@echo "Cleaning up..."
	docker-compose down -v
	rm -rf localstack_data/
	rm -rf dagster_home/
	rm -rf data/
	@echo "Clean complete"

test:
	@echo "Running tests..."
	pytest tests/ -v
