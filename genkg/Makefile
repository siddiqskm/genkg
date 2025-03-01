.PHONY: install run test lint format clean help

# Variables
PYTHON = poetry run python
UVICORN = poetry run uvicorn
APP_MODULE = app.main:app

help:
	@echo "Available commands:"
	@echo "install  - Install dependencies using Poetry"
	@echo "run      - Run the FastAPI server"
	@echo "test     - Run tests"
	@echo "lint     - Run linting checks"
	@echo "format   - Format code using black and isort"
	@echo "clean    - Clean up python cache files"
	@echo "setup    - Initial setup (create logs dir, etc)"

install:
	poetry install

run:
	$(UVICORN) $(APP_MODULE) --reload --host 0.0.0.0 --port 8000

test:
	poetry run pytest

lint:
	poetry run flake8 app
	poetry run black app --check
	poetry run isort app --check-only

format:
	poetry run black app
	poetry run isort app

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete

setup:
	mkdir -p logs