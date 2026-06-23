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
test:
	@echo "$(YELLOW)$(BOLD)Running tests$(RESET)"
	pytest

.PHONY: cov
cov: | install
	@echo "$(YELLOW)$(BOLD)Running tests with coverage$(RESET)"
	pytest --cov=src --cov-report=term-missing --cov-report=html:htmlcov


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
	@docker exec kafka kafka-topics --create --if-not-exists --topic summary-completed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
	@docker exec kafka kafka-topics --create --if-not-exists --topic reload-index --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
	@docker exec kafka kafka-topics --create --if-not-exists --topic frontend-message --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
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
	@docker exec kafka kafka-topics --delete --topic summary-completed --bootstrap-server localhost:9092 2>/dev/null || true
	@docker exec kafka kafka-topics --delete --topic reload-index --bootstrap-server localhost:9092 2>/dev/null || true
	@docker exec kafka kafka-topics --delete --topic frontend-message --bootstrap-server localhost:9092 2>/dev/null || true
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


# ──────────────────────────────────────────────────────────────────────────────
# k8s deployment targets — aligned with RapporteurRAG / RapporteurFrontEnd
#
# A "consumer group" = one helm release inside the shared `consumers`
# namespace. Override RELEASE_NAME to deploy additional groups:
#   make k8s_deploy RELEASE_NAME=viperine
#   make k8s_deploy RELEASE_NAME=indexing
# ──────────────────────────────────────────────────────────────────────────────
REGISTRY      ?= 192.168.1.3:5000
K8S_IMAGE     ?= rapporteur-consumer-viperine
K8S_VERSION   ?= 0.0.1
K8S_NS        ?= consumers
HELM_CHART    ?= ./helm/rapporteur-consumer-group
RELEASE_NAME  ?= viperine
DOCKER_BUILD_ENV ?= DOCKER_BUILDKIT=0

# Kafka broker (exec target: pod/container in the kafka namespace)
KAFKA_POD       ?= kafka-0
KAFKA_NS        ?= kafka
KAFKA_BOOTSTRAP ?= kafka-headless.kafka.svc.cluster.local:9092

# All topics owned by this pipeline (dot-namespaced, Kafka-safe)
VIPERINE_TOPICS := \
	viperine.web-scrape-requests \
	viperine.content-extracted \
	viperine.summarise-document \
	viperine.summary-completed \
	viperine.session-complete \
	viperine.dead-letter-queue \
	viperine.frontend-message


.PHONY: k8s_prereqs
k8s_prereqs: ## One-time bootstrap: hostPath dir, namespace, copy gcp-credentials from rag-service
	@echo "$(YELLOW)$(BOLD)Setting up k8s prerequisites...$(RESET)"
	@sudo mkdir -p /mnt/workspace_2/$(RELEASE_NAME)-cache
	@sudo chmod 777 /mnt/workspace_2/$(RELEASE_NAME)-cache
	@kubectl get ns $(K8S_NS) >/dev/null 2>&1 \
		|| kubectl create ns $(K8S_NS)
	@kubectl get secret gcp-credentials -n $(K8S_NS) >/dev/null 2>&1 \
		|| kubectl get secret gcp-credentials -n rag-service -o yaml \
			| sed "s/namespace: rag-service/namespace: $(K8S_NS)/" \
			| kubectl apply -f -
	@echo "$(GREEN)$(BOLD)Prereqs ready in namespace $(K8S_NS).$(RESET)"


.PHONY: kafka_create_topics
kafka_create_topics: ## Create all viperine. namespaced Kafka topics (idempotent --if-not-exists)
	@echo "$(YELLOW)$(BOLD)Creating Viperine Kafka topics on $(KAFKA_BOOTSTRAP)...$(RESET)"
	@for topic in $(VIPERINE_TOPICS); do \
		kubectl -n $(KAFKA_NS) exec $(KAFKA_POD) -- \
			kafka-topics \
			--bootstrap-server $(KAFKA_BOOTSTRAP) \
			--create --if-not-exists \
			--topic $$topic \
			--partitions 1 \
			--replication-factor 1 2>&1 | grep -v "^WARNING" ; \
		echo "  $(GREEN)✓$(RESET) $$topic" ; \
	done
	@echo "$(GREEN)$(BOLD)All topics ready.$(RESET)"


.PHONY: kafka_list_topics
kafka_list_topics: ## List all viperine. topics currently on the broker
	@kubectl -n $(KAFKA_NS) exec $(KAFKA_POD) -- \
		kafka-topics \
		--bootstrap-server $(KAFKA_BOOTSTRAP) \
		--list 2>/dev/null | grep "^viperine\."


.PHONY: kafka_delete_old_topics
kafka_delete_old_topics: ## Delete the legacy (non-namespaced) topics once migration is confirmed
	@echo "$(YELLOW)$(BOLD)Deleting legacy topics — only run after migration is validated!$(RESET)"
	@for topic in \
		web-scrape-requests \
		content-extracted \
		summarise-document \
		summary-completed \
		session-complete \
		dead-letter-queue \
		frontend-message; do \
		kubectl -n $(KAFKA_NS) exec $(KAFKA_POD) -- \
			kafka-topics \
			--bootstrap-server $(KAFKA_BOOTSTRAP) \
			--delete --if-exists \
			--topic $$topic 2>&1 | grep -v "^WARNING" ; \
		echo "  $(YELLOW)✗$(RESET) $$topic deleted" ; \
	done
	@echo "$(GREEN)$(BOLD)Legacy topics removed.$(RESET)"


.PHONY: k8s_build
k8s_build: ## Build container image and tag for local registry
	@echo "$(YELLOW)$(BOLD)Building $(REGISTRY)/$(K8S_IMAGE):$(K8S_VERSION)...$(RESET)"
	@$(DOCKER_BUILD_ENV) docker build -f Dockerfile -t $(REGISTRY)/$(K8S_IMAGE):$(K8S_VERSION) .
	@echo "$(GREEN)$(BOLD)Build complete.$(RESET)"


.PHONY: k8s_push
k8s_push: ## Push image to local registry
	@echo "$(YELLOW)$(BOLD)Pushing $(REGISTRY)/$(K8S_IMAGE):$(K8S_VERSION)...$(RESET)"
	@docker push $(REGISTRY)/$(K8S_IMAGE):$(K8S_VERSION)
	@echo "$(GREEN)$(BOLD)Push complete.$(RESET)"


.PHONY: k8s_install
k8s_install: ## Helm install/upgrade $(RELEASE_NAME) into namespace $(K8S_NS)
	@echo "$(YELLOW)$(BOLD)Deploying $(RELEASE_NAME) via Helm...$(RESET)"
	@helm upgrade --install $(RELEASE_NAME) $(HELM_CHART) \
		--namespace $(K8S_NS) --create-namespace \
		--set image.tag=$(K8S_VERSION)
	@echo "$(GREEN)$(BOLD)Release $(RELEASE_NAME) installed.$(RESET)"


.PHONY: k8s_rollout
k8s_rollout: ## Nvidia-safe rollout: scale→0, purge stuck pods, helm upgrade (workaround for k3s/containerd nvidia-runtime)
	@echo "$(YELLOW)$(BOLD)[1/3] Scaling down all $(RELEASE_NAME) deployments to 0...$(RESET)"
	@kubectl -n $(K8S_NS) scale deploy -l consumer-group=$(RELEASE_NAME) --replicas=0
	@echo "Waiting 15s for graceful shutdown..."
	@sleep 15
	@STUCK=$$(kubectl -n $(K8S_NS) get pods -l consumer-group=$(RELEASE_NAME) -o name 2>/dev/null | wc -l | tr -d ' '); \
	if [ "$$STUCK" -gt 0 ]; then \
		echo "$(YELLOW)⚠  $$STUCK pod(s) still terminating — force-deleting (nvidia-container-runtime workaround)...$(RESET)"; \
		kubectl -n $(K8S_NS) get pods -l consumer-group=$(RELEASE_NAME) -o name \
			| xargs -r kubectl -n $(K8S_NS) delete --force --grace-period=0; \
		sleep 3; \
	else \
		echo "$(GREEN)✓ All pods terminated cleanly.$(RESET)"; \
	fi
	@echo "$(YELLOW)$(BOLD)[2/3] Upgrading helm release $(RELEASE_NAME)...$(RESET)"
	@helm upgrade --install $(RELEASE_NAME) $(HELM_CHART) \
		--namespace $(K8S_NS) --create-namespace \
		--set image.tag=$(K8S_VERSION)
	@echo "$(GREEN)$(BOLD)[3/3] Helm upgrade complete — pods starting fresh.$(RESET)"


.PHONY: k8s_deploy
k8s_deploy: k8s_build k8s_push k8s_rollout k8s_status ## Full release pipeline: build → push → safe-rollout → status


.PHONY: k8s_status
k8s_status: ## Show rollout status and pods/svcs for this consumer group
	@for d in $$(kubectl -n $(K8S_NS) get deploy -l consumer-group=$(RELEASE_NAME) -o name); do \
		kubectl -n $(K8S_NS) rollout status $$d --timeout=180s ; \
	done
	@echo ""
	@kubectl -n $(K8S_NS) get pods,svc -l consumer-group=$(RELEASE_NAME)


.PHONY: k8s_logs
k8s_logs: ## Tail logs from all pods in this consumer group (Ctrl-C to stop)
	@kubectl -n $(K8S_NS) logs -f -l consumer-group=$(RELEASE_NAME) --all-containers=true --max-log-requests=10


.PHONY: k8s_restart
k8s_restart: ## Rolling restart of every deployment in this consumer group
	@kubectl -n $(K8S_NS) rollout restart deploy -l consumer-group=$(RELEASE_NAME)


.PHONY: k8s_shell
k8s_shell: ## Open a shell inside one consumer pod (override ROLE=<role-kebabcase>)
	@kubectl -n $(K8S_NS) exec -it deploy/$(RELEASE_NAME)-$(or $(ROLE),web-scraper) -- /bin/bash


.PHONY: k8s_smoke
k8s_smoke: ## E2E smoke test: health probes → RAG trigger → pipeline Kafka flow → indexing
	@RELEASE_NAME=$(RELEASE_NAME) K8S_NS=$(K8S_NS) \
		RAG_HOST=$(or $(RAG_HOST),192.168.1.3) \
		RAG_VHOST=$(or $(RAG_VHOST),rag.rapporteur.ai) \
		ENRICH=$(or $(ENRICH),true) \
		bash scripts/smoke_test_e2e.sh

.PHONY: k8s_smoke_search
k8s_smoke_search: ## Search-only smoke test (no web scraping): health probes + RAG response only
	@RELEASE_NAME=$(RELEASE_NAME) K8S_NS=$(K8S_NS) \
		RAG_HOST=$(or $(RAG_HOST),192.168.1.3) \
		RAG_VHOST=$(or $(RAG_VHOST),rag.rapporteur.ai) \
		ENRICH=false \
		bash scripts/smoke_test_e2e.sh


.PHONY: k8s_uninstall
k8s_uninstall: ## Uninstall the helm release (leaves namespace and secrets intact)
	@echo "$(YELLOW)$(BOLD)Uninstalling $(RELEASE_NAME) from $(K8S_NS)...$(RESET)"
	@helm uninstall $(RELEASE_NAME) -n $(K8S_NS) || true
	@echo "$(GREEN)Done. Namespace $(K8S_NS) and secrets preserved.$(RESET)"
	@echo "To delete the namespace: kubectl delete ns $(K8S_NS)"


# ──────────────────────────────────────────────────────────────────────────────
# Monitoring targets
# ──────────────────────────────────────────────────────────────────────────────
PROMETHEUS_URL ?= http://192.168.1.3:30090
GRAFANA_URL    ?= http://192.168.1.3:30030

.PHONY: monitoring_deploy
monitoring_deploy: ## Deploy monitoring config to cluster (idempotent, reload-only)
	@bash scripts/deploy_cluster_monitoring.sh

.PHONY: monitoring_status
monitoring_status: ## Check Prometheus target status for Viperine consumers
	@echo "$(YELLOW)$(BOLD)Querying Prometheus targets...$(RESET)"
	@curl -s $(PROMETHEUS_URL)/api/v1/targets | python3 -c "\
	import json, sys; \
	data = json.load(sys.stdin); \
	targets = data.get('data', {}).get('activeTargets', []); \
	viperine = [t for t in targets if 'viperine' in t.get('labels', {}).get('job', '')]; \
	print(f'Viperine targets: {len(viperine)}'); \
	[print(f\"  {'✓' if t['health']=='up' else '✗'} {t['labels'].get('job','?'):30s} {t['health']}\") for t in viperine] \
	"

.PHONY: monitoring_reload
monitoring_reload: ## Reload Prometheus config (safe — never restart)
	@curl -s -X POST $(PROMETHEUS_URL)/-/reload && echo "$(GREEN)✓ Prometheus reloaded$(RESET)"

.PHONY: monitoring_open
monitoring_open: ## Print monitoring URLs
	@echo "Grafana:    $(GRAFANA_URL)"
	@echo "Prometheus: $(PROMETHEUS_URL)"
	@echo "Dashboard:  $(GRAFANA_URL)/d/viperine-pipeline/viperine-pipeline-overview"
	@echo "Targets:    $(PROMETHEUS_URL)/targets"
	@echo "Alerts:     $(PROMETHEUS_URL)/alerts"
