# Broker Contact Scraper - Makefile
.PHONY: help setup install run run-bg logs stop clean clean-all

# Default target
.DEFAULT_GOAL := help

# Variables
VENV_DIR := venv
PYTHON := python3
PIP := $(VENV_DIR)/bin/pip
PYTHON_VENV := $(VENV_DIR)/bin/python
SCRAPER := scraper.py
LOG_FILE := scraper.log
PID_FILE := scraper.pid
CSV_FILE := broker_contacts.csv

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

## Show this help message
help:
	@echo "$(BLUE)Broker Contact Scraper - Available Commands:$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-15s$(NC) %s\n", $$1, $$2}'

## Create virtual environment and install dependencies
setup: $(VENV_DIR)
	@echo "$(GREEN)✓ Virtual environment created$(NC)"
	@echo "$(YELLOW)Installing dependencies...$(NC)"
	@$(PIP) install -r requirements.txt
	@echo "$(GREEN)✓ Setup complete!$(NC)"
	@echo "$(BLUE)To activate venv: source $(VENV_DIR)/bin/activate$(NC)"

## Install/update Python dependencies
install: $(VENV_DIR)
	@echo "$(YELLOW)Installing dependencies...$(NC)"
	@$(PIP) install -r requirements.txt
	@echo "$(GREEN)✓ Dependencies installed$(NC)"

## Run the scraper
run: $(VENV_DIR)
	@echo "$(BLUE)Starting scraper...$(NC)"
	@$(PYTHON_VENV) $(SCRAPER)

## Run scraper in background (useful for long-running scrapes)
run-bg: $(VENV_DIR)
	@echo "$(BLUE)Starting scraper in background...$(NC)"
	@nohup $(PYTHON_VENV) $(SCRAPER) > $(LOG_FILE) 2>&1 & echo $$! > $(PID_FILE)
	@echo "$(GREEN)✓ Scraper started in background (PID: $$(cat $(PID_FILE)))$(NC)"
	@echo "$(YELLOW)Use 'make logs' to view output$(NC)"
	@echo "$(YELLOW)Use 'make stop' to stop the process$(NC)"

## View background process logs
logs:
	@if [ -f $(LOG_FILE) ]; then \
		tail -f $(LOG_FILE); \
	else \
		echo "$(RED)No log file found. Start the scraper with 'make run-bg' first.$(NC)"; \
	fi

## Stop background scraper process
stop:
	@if [ -f $(PID_FILE) ]; then \
		PID=$$(cat $(PID_FILE)); \
		if kill -0 $$PID 2>/dev/null; then \
			echo "$(YELLOW)Stopping scraper process (PID: $$PID)...$(NC)"; \
			kill $$PID; \
			rm -f $(PID_FILE); \
			echo "$(GREEN)✓ Scraper stopped$(NC)"; \
		else \
			echo "$(RED)Process $$PID not found$(NC)"; \
			rm -f $(PID_FILE); \
		fi; \
	else \
		echo "$(RED)No PID file found. Is the scraper running in background?$(NC)"; \
	fi

## Remove generated files and cache
clean:
	@echo "$(YELLOW)Cleaning generated files...$(NC)"
	@rm -f $(LOG_FILE) $(PID_FILE) $(CSV_FILE)
	@rm -rf __pycache__ *.pyc
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)✓ Clean complete$(NC)"

## Remove venv, generated files, and cache (full cleanup)
clean-all: clean
	@echo "$(YELLOW)Removing virtual environment...$(NC)"
	@rm -rf $(VENV_DIR)
	@echo "$(GREEN)✓ Full cleanup complete$(NC)"

## Check if virtual environment exists
$(VENV_DIR):
	@echo "$(YELLOW)Creating virtual environment...$(NC)"
	@$(PYTHON) -m venv $(VENV_DIR)

## Check Python version
check-python:
	@echo "$(BLUE)Python version:$(NC)"
	@$(PYTHON) --version
	@echo "$(BLUE)Pip version:$(NC)"
	@$(PYTHON) -m pip --version

## Show project status
status:
	@echo "$(BLUE)Project Status:$(NC)"
	@if [ -d $(VENV_DIR) ]; then \
		echo "$(GREEN)✓ Virtual environment: $(VENV_DIR)$(NC)"; \
	else \
		echo "$(RED)✗ Virtual environment: $(VENV_DIR) (not found)$(NC)"; \
	fi
	@if [ -f requirements.txt ]; then \
		echo "$(GREEN)✓ Dependencies file: requirements.txt$(NC)"; \
	else \
		echo "$(RED)✗ Dependencies file: requirements.txt (not found)$(NC)"; \
	fi
	@if [ -f $(SCRAPER) ]; then \
		echo "$(GREEN)✓ Scraper script: $(SCRAPER)$(NC)"; \
	else \
		echo "$(RED)✗ Scraper script: $(SCRAPER) (not found)$(NC)"; \
	fi
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE) 2>/dev/null) 2>/dev/null; then \
		echo "$(GREEN)✓ Background process running (PID: $$(cat $(PID_FILE)))$(NC)"; \
	else \
		echo "$(YELLOW)○ No background process running$(NC)"; \
	fi
	@if [ -f $(CSV_FILE) ]; then \
		echo "$(GREEN)✓ Results file exists: $(CSV_FILE) ($$(wc -l < $(CSV_FILE)) lines)$(NC)"; \
	else \
		echo "$(YELLOW)○ No results file found$(NC)"; \
	fi
