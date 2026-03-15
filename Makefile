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


# Docker/Kubernetes targets
ACR_NAME ?= ragserviceneacr6881
REGION ?= northeurope
RESOURCE_GROUP ?= rapporteur-ne-rg
IMAGE_NAME ?= scrape-consumer
VERSION ?= v1.0.0
ACR_LOGIN_SERVER = $(ACR_NAME).azurecr.io
FULL_IMAGE = $(ACR_LOGIN_SERVER)/$(IMAGE_NAME):$(VERSION)
LATEST_IMAGE = $(ACR_LOGIN_SERVER)/$(IMAGE_NAME):latest


.PHONY: docker_build
docker_build: ## Build Docker image for consumers
	@echo "$(YELLOW)$(BOLD)Building Docker image...$(RESET)"
	docker build -t $(IMAGE_NAME):$(VERSION) .
	@echo "✓ Docker image built: $(IMAGE_NAME):$(VERSION)"


.PHONY: docker_tag
docker_tag: ## Tag Docker image for ACR
	@echo "$(YELLOW)$(BOLD)Tagging images for ACR...$(RESET)"
	docker tag $(IMAGE_NAME):$(VERSION) $(FULL_IMAGE)
	docker tag $(IMAGE_NAME):$(VERSION) $(LATEST_IMAGE)
	@echo "✓ Tagged: $(FULL_IMAGE)"
	@echo "✓ Tagged: $(LATEST_IMAGE)"


.PHONY: docker_push
docker_push: ## Push Docker image to ACR
	@echo "$(YELLOW)$(BOLD)Pushing to ACR...$(RESET)"
	@echo "Logging in to ACR: $(ACR_LOGIN_SERVER)..."
	@az acr login --name $(ACR_NAME)
	docker push $(FULL_IMAGE)
	docker push $(LATEST_IMAGE)
	@echo "✓ Successfully pushed to ACR!"
	@echo ""
	@echo "Images available at:"
	@echo "  $(FULL_IMAGE)"
	@echo "  $(LATEST_IMAGE)"


.PHONY: docker_build_push
docker_build_push: docker_build docker_tag docker_push ## Build, tag, and push Docker image to ACR
	@echo ""
	@echo "$(GREEN)$(BOLD)Docker image built and pushed successfully!$(RESET)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Update k8s/05-consumer-deployments.yaml with image: $(LATEST_IMAGE)"
	@echo "  2. Deploy to AKS: kubectl apply -f k8s/"


.PHONY: azure_login
azure_login: ## Login to Azure
	@echo "$(YELLOW)$(BOLD)Logging in to Azure...$(RESET)"
	@az login
	@echo "✓ Azure login successful"


# Kafka Docker images
KAFKA_VERSION ?= 7.5.0
KAFKA_IMAGE = kafka
ZOOKEEPER_IMAGE = zookeeper
KAFKA_FULL_IMAGE = $(ACR_LOGIN_SERVER)/$(KAFKA_IMAGE):$(KAFKA_VERSION)
KAFKA_LATEST_IMAGE = $(ACR_LOGIN_SERVER)/$(KAFKA_IMAGE):latest
ZOOKEEPER_FULL_IMAGE = $(ACR_LOGIN_SERVER)/$(ZOOKEEPER_IMAGE):$(KAFKA_VERSION)
ZOOKEEPER_LATEST_IMAGE = $(ACR_LOGIN_SERVER)/$(ZOOKEEPER_IMAGE):latest


.PHONY: kafka_docker_build
kafka_docker_build: ## Build Kafka Docker images
	@echo "$(YELLOW)$(BOLD)Building Kafka Docker image...$(RESET)"
	docker build --platform linux/amd64 -f Dockerfile.kafka -t $(KAFKA_IMAGE):$(KAFKA_VERSION) .
	@echo "$(YELLOW)$(BOLD)Building ZooKeeper Docker image...$(RESET)"
	docker build --platform linux/amd64 -f Dockerfile.zookeeper -t $(ZOOKEEPER_IMAGE):$(KAFKA_VERSION) .
	@echo "✓ Kafka images built"


.PHONY: kafka_docker_tag
kafka_docker_tag: ## Tag Kafka Docker images for ACR
	@echo "$(YELLOW)$(BOLD)Tagging Kafka images for ACR...$(RESET)"
	docker tag $(KAFKA_IMAGE):$(KAFKA_VERSION) $(KAFKA_FULL_IMAGE)
	docker tag $(KAFKA_IMAGE):$(KAFKA_VERSION) $(KAFKA_LATEST_IMAGE)
	docker tag $(ZOOKEEPER_IMAGE):$(KAFKA_VERSION) $(ZOOKEEPER_FULL_IMAGE)
	docker tag $(ZOOKEEPER_IMAGE):$(KAFKA_VERSION) $(ZOOKEEPER_LATEST_IMAGE)
	@echo "✓ Tagged: $(KAFKA_FULL_IMAGE)"
	@echo "✓ Tagged: $(KAFKA_LATEST_IMAGE)"
	@echo "✓ Tagged: $(ZOOKEEPER_FULL_IMAGE)"
	@echo "✓ Tagged: $(ZOOKEEPER_LATEST_IMAGE)"


.PHONY: kafka_docker_push
kafka_docker_push: ## Push Kafka Docker images to ACR
	@echo "$(YELLOW)$(BOLD)Pushing Kafka images to ACR...$(RESET)"
	@echo "Logging in to ACR: $(ACR_LOGIN_SERVER)..."
	@az acr login --name $(ACR_NAME)
	docker push $(KAFKA_FULL_IMAGE)
	docker push $(KAFKA_LATEST_IMAGE)
	docker push $(ZOOKEEPER_FULL_IMAGE)
	docker push $(ZOOKEEPER_LATEST_IMAGE)
	@echo "✓ Successfully pushed Kafka images to ACR!"
	@echo ""
	@echo "Images available at:"
	@echo "  $(KAFKA_FULL_IMAGE)"
	@echo "  $(KAFKA_LATEST_IMAGE)"
	@echo "  $(ZOOKEEPER_FULL_IMAGE)"
	@echo "  $(ZOOKEEPER_LATEST_IMAGE)"


.PHONY: kafka_docker_build_push
kafka_docker_build_push: kafka_docker_build kafka_docker_tag kafka_docker_push ## Build, tag, and push Kafka images to ACR
	@echo ""
	@echo "$(GREEN)$(BOLD)Kafka images built and pushed successfully!$(RESET)"


.PHONY: docker_build_all
docker_build_all: docker_build_push kafka_docker_build_push ## Build and push all Docker images (consumers + Kafka)
	@echo ""
	@echo "$(GREEN)$(BOLD)All Docker images built and pushed successfully!$(RESET)"

# ============================================================================
# RAG + Frontend Services
# ============================================================================

.PHONY: services_build
services_build: ## Build RAG and frontend service images
	@echo "$(YELLOW)$(BOLD)Building RAG and frontend service images...$(RESET)"
	@docker-compose -f docker-compose.services.yml build
	@echo "$(GREEN)$(BOLD)Build complete.$(RESET)"

.PHONY: services_up
services_up: ## Start RAG and frontend services (detached)
	@echo "$(YELLOW)$(BOLD)Starting RAG and frontend services...$(RESET)"
	@docker-compose -f docker-compose.services.yml up -d
	@echo ""
	@echo "$(GREEN)$(BOLD)Services started:$(RESET)"
	@echo "  RAG service:      http://localhost:8000"
	@echo "  Frontend service: http://localhost:5001"
	@echo ""
	@echo "Logs: make services_logs"
	@echo "Stop:  make services_down"

.PHONY: services_down
services_down: ## Stop and remove RAG and frontend service containers
	@echo "$(YELLOW)$(BOLD)Stopping services...$(RESET)"
	@docker-compose -f docker-compose.services.yml down
	@echo "$(GREEN)$(BOLD)Services stopped.$(RESET)"

.PHONY: services_restart
services_restart: services_down services_up ## Restart RAG and frontend services

.PHONY: services_logs
services_logs: ## Tail logs from RAG and frontend services
	@docker-compose -f docker-compose.services.yml logs -f

.PHONY: services_logs_rag
services_logs_rag: ## Tail RAG service logs only
	@docker-compose -f docker-compose.services.yml logs -f rag-service

.PHONY: services_logs_frontend
services_logs_frontend: ## Tail frontend service logs only
	@docker-compose -f docker-compose.services.yml logs -f frontend-service

.PHONY: services_ps
services_ps: ## Show status of RAG and frontend service containers
	@docker-compose -f docker-compose.services.yml ps

.PHONY: services_rebuild
services_rebuild: ## Rebuild images and restart services (no cache)
	@echo "$(YELLOW)$(BOLD)Rebuilding services (no cache)...$(RESET)"
	@docker-compose -f docker-compose.services.yml build --no-cache
	@docker-compose -f docker-compose.services.yml up -d
	@echo "$(GREEN)$(BOLD)Services rebuilt and started.$(RESET)"
