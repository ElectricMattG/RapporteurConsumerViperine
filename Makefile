VIRTUALENV_NAME := $(shell basename $(CURDIR))

PIP_COMPILE_ARGS = -v -U --no-emit-index-url --no-emit-trusted-host --strip-extras --resolver=backtracking

PYTHON_VERSION = 3.12 # keep minor versions syncd


.PHONY: _python_version
_python_version:
	@if [ -z $(PYTHON_VERSION) ]; then \
		echo "$(RED)$(BOLD)This is the python verion $(PYTHON_VERSION)$(RESET)"; \
		exit 1; \
	fi


.PHONY: pyenv
pyenv: | _python_version # create a python virtual env
	@echo "$(YELLOW)$(BOLD)Creating a virtual environment$(RESET)"
	@if ! pyenv versions --bare | grep -q $(PYTHON_VERSION); then \
		echo "Python version $(PYTHON_VERSION) is not installed."; \
		echo "Installing this version of Python..."; \
		pyenv install -s $(PYTHON_VERSION); \
	fi
	@if ! pyenv versions --bare | grep -q $(VIRTUALENV_NAME); then \
		pyenv virtualenv $(PYTHON_VERSION) $(VIRTUALENV_NAME); else \
		echo "Virtual environment $(VIRTUALENV_NAME) already exists."; \
	fi
	@pyenv local $(VIRTUALENV_NAME)
	@pip install --upgrade pip


.PHONY: pre-commit
pre-commit:
	@pip install -q pre-commit
	@pre-commit install


.PHONY: check
check: | pre-commit
	@echo "$(YELLOW)$(BOLD)Running checks (pre-commit)$(RESET)"
	pre-commit run --all-files


.PHONY: pip-tools
pip-tools:
	@echo "$(YELLOW)$(BOLD)Installing pip-tools (pre-commit)$(RESET)"
	@pip install -q pip-tools


%.txt: %.in | pip-tools # Compile specified requirements (pip-compile).
	@echo "$(YELLOW)$(BOLD)Compiling $*.txt$(RESET)"
	CUSTOM_COMPILE_COMMAND="make update" pip-compile $(PIP_COMPILE_ARGS) $*.in


.PHONY: requirements
requirements: requirements.in dev-requirements.in | pip-tools
	@echo "$(YELLOW)$(BOLD)Compiling requirements$(RESET)"
	CUSTOM_COMPILE_COMMAND="make update" pip-compile $(PIP_COMPILE_ARGS) requirements.in --output-file requirements.txt
	CUSTOM_COMPILE_COMMAND="make update" pip-compile $(PIP_COMPILE_ARGS) dev-requirements.in --output-file dev-requirements.txt


.PHONY: sync
sync: requirements.txt dev-requirements.txt | pip-tools
	@echo "$(YELLOW)$(BOLD)Syncing dependencies$(RESET)"
	pip-sync requirements.txt dev-requirements.txt


.PHONY: install
install: requirements.txt dev-requirements.txt
	@echo "$(YELLOW)$(BOLD)Installing dependencies$(RESET)"
	pip install \
	-r requirements.txt -r dev-requirements.txt
	pip install -e .
	@echo "$(YELLOW)$(BOLD)Installing Playwright browser binaries$(RESET)"
	python -m playwright install chromium


.PHONY: test
test: | install
	@echo "$(YELLOW)$(BOLD)Running tests$(RESET)"
	pytest


.PHONY: gcp_gemini
gcp_gemini: ## Setup GCP authentication and environment for Gemini API
	@echo "$(YELLOW)$(BOLD)Setting up GCP Gemini environment$(RESET)"
	@echo "Checking GCP authentication..."
	@if ! gcloud auth application-default print-access-token > /dev/null 2>&1; then \
		echo "Setting up Application Default Credentials..."; \
		gcloud auth application-default login --no-launch-browser; \
	else \
		echo "✓ Already authenticated with Application Default Credentials"; \
	fi
	@echo ""
	@echo "Checking GCP project..."
	@if [ -z "$${PROJECT_ID}" ]; then \
		PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
		if [ -z "$${PROJECT_ID}" ]; then \
			echo "ERROR: No PROJECT_ID set. Please set it with:"; \
			echo "  export PROJECT_ID=your-project-id"; \
			exit 1; \
		fi; \
	fi; \
	echo "✓ Project ID: $${PROJECT_ID}"
	@echo ""
	@echo "Enabling required APIs..."
	@gcloud services enable aiplatform.googleapis.com generativelanguage.googleapis.com --project=$${PROJECT_ID:-$$(gcloud config get-value project)} 2>/dev/null || true
	@echo "✓ APIs enabled"
	@echo ""
	@echo "$(GREEN)$(BOLD)GCP Gemini setup complete!$(RESET)"
	@echo ""
	@echo "Export these environment variables in your shell:"
	@echo "  export PROJECT_ID=$${PROJECT_ID:-$$(gcloud config get-value project 2>/dev/null)}"
	@echo "  export LOCATION=us-central1"
	@echo ""
	@echo "Or run commands with them:"
	@echo "  PROJECT_ID=$${PROJECT_ID:-$$(gcloud config get-value project 2>/dev/null)} LOCATION=us-central1 python3 your_script.py"


.PHONY: kafka_start
kafka_start: ## Start Kafka and Zookeeper containers
	@echo "$(YELLOW)$(BOLD)Starting Kafka services...$(RESET)"
	@docker-compose -f docker-compose.kafka.yml up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 8
	@echo "✓ Kafka services started"
	@echo ""
	@echo "Kafka UI available at: http://localhost:8090"


.PHONY: kafka_stop
kafka_stop: ## Stop Kafka and Zookeeper containers
	@echo "$(YELLOW)$(BOLD)Stopping Kafka services...$(RESET)"
	@docker-compose -f docker-compose.kafka.yml down
	@echo "✓ Kafka services stopped"


.PHONY: kafka_topics
kafka_topics: ## Create all required Kafka topics
	@echo "$(YELLOW)$(BOLD)Creating Kafka topics...$(RESET)"
	@docker exec kafka kafka-topics --create --if-not-exists --topic web-scrape-requests --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
	@docker exec kafka kafka-topics --create --if-not-exists --topic content-extracted --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
	@docker exec kafka kafka-topics --create --if-not-exists --topic citation-extraction --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
	@docker exec kafka kafka-topics --create --if-not-exists --topic chunk-embedding --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
	@docker exec kafka kafka-topics --create --if-not-exists --topic qa-generation --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
	@docker exec kafka kafka-topics --create --if-not-exists --topic dead-letter-queue --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
	@echo "✓ All topics created"
	@echo ""
	@echo "Listing topics:"
	@docker exec kafka kafka-topics --list --bootstrap-server localhost:9092


.PHONY: kafka_setup
kafka_setup: kafka_start kafka_topics ## Start Kafka and create all topics
	@echo ""
	@echo "$(GREEN)$(BOLD)Kafka setup complete!$(RESET)"
	@echo "Bootstrap servers: localhost:9092"
	@echo "Kafka UI: http://localhost:8090"


.PHONY: kafka_flush
kafka_flush: ## Flush all Kafka topics and clear cache
	@echo "$(YELLOW)$(BOLD)Flushing Kafka topics and cache...$(RESET)"
	@echo "Deleting and recreating topics..."
	@docker exec kafka kafka-topics --delete --topic web-scrape-requests --bootstrap-server localhost:9092 2>/dev/null || true
	@docker exec kafka kafka-topics --delete --topic content-extracted --bootstrap-server localhost:9092 2>/dev/null || true
	@docker exec kafka kafka-topics --delete --topic citation-extraction --bootstrap-server localhost:9092 2>/dev/null || true
	@docker exec kafka kafka-topics --delete --topic chunk-embedding --bootstrap-server localhost:9092 2>/dev/null || true
	@docker exec kafka kafka-topics --delete --topic qa-generation --bootstrap-server localhost:9092 2>/dev/null || true
	@docker exec kafka kafka-topics --delete --topic dead-letter-queue --bootstrap-server localhost:9092 2>/dev/null || true
	@sleep 3
	@echo "Recreating topics..."
	@$(MAKE) kafka_topics
	@echo "Clearing cache..."
	@rm -rf cache/web_scrapes/req_*
	@echo "✓ Kafka topics flushed and cache cleared"
