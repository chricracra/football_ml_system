.PHONY: help install setup test train predict clean

help:
	@echo "Football ML System - Make Commands"
	@echo ""
	@echo "install     Install dependencies"
	@echo "setup       Setup project structure"
	@echo "test        Run tests"
	@echo "train       Train model"
	@echo "predict     Run predictions"
	@echo "daily       Run daily pipeline"
	@echo "clean       Clean temporary files"
	@echo "docker-up   Start Docker containers"
	@echo "docker-down Stop Docker containers"

install:
	pip install -r requirements.txt
	pre-commit install

setup:
	mkdir -p data/{raw,processed,cache,database,exports,predictions}
	mkdir -p models/{xgboost,random_forest,ensemble,metadata}
	mkdir -p logs notebooks scripts tests
	mkdir -p config docs
	cp .env.example .env
	@echo "Please edit .env file with your API keys"

test:
	pytest tests/ -v --cov=src

train:
	python scripts/train_model.py --model xgboost --all

predict:
	python scripts/predict_matches.py --date $(shell date +%Y-%m-%d)

daily:
	python scripts/run_daily_pipeline.py

clean:
	rm -rf data/cache/*
	rm -rf data/exports/*
	rm -rf __pycache__ src/__pycache__ tests/__pycache__
	rm -rf .pytest_cache .coverage
	find . -type f -name "*.pyc" -delete

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f ml-pipeline

backtest:
	python scripts/backtest_strategy.py --start-date 2023-01-01 --end-date 2023-12-31
