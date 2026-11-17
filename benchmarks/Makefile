SHELL := /bin/bash
SCRIPTS_DIR := scripts
MANIFESTS_DIR := manifests
HARNESS_DIR := harness
MGMT_CONTEXT := kind-capi-bench
WORKLOAD_CLUSTER := bench-cluster
# aws-vault profile for the bench AWS account, and the EC2 key pair CAPA
# attaches to bench nodes. Both must be set in your environment.
export BENCH_AWS_PROFILE AWS_SSH_KEY_NAME
VENV_PYTHON := $(HARNESS_DIR)/.venv/bin/python
# Benchmark parameters (override e.g. `make bench-all PROFILE=heavy-constrained RUNS=1 PODS=100`)
PROFILE ?= unconstrained
RUNS ?= 3
PODS ?=
PODS_FLAG = $(if $(PODS),--pods $(PODS),)
# Delete the workload/EKS cluster when a bench target finishes. Set TEARDOWN=0
# to keep it up for another run in the same session (bench-all does this).
TEARDOWN ?= 1

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

# ─── Infrastructure ──────────────────────────────────────────────────────────

.PHONY: kubeconfig
kubeconfig: ## Ensure the kind mgmt cluster context exists in kubeconfig (preserves current context)
	@CUR=$$(kubectl config current-context 2>/dev/null || true); \
	kind export kubeconfig --name capi-bench; \
	if [ -n "$$CUR" ] && [ "$$CUR" != "$(MGMT_CONTEXT)" ]; then kubectl config use-context "$$CUR" >/dev/null; fi

.PHONY: workload-apply
workload-apply: kubeconfig ## Apply the workload cluster manifest (propagates MD/annotation changes)
	@: "$${AWS_SSH_KEY_NAME:?set it to the EC2 key pair for bench nodes}"
	envsubst '$${AWS_SSH_KEY_NAME}' < $(MANIFESTS_DIR)/capa-workload-cluster.yaml | kubectl --context $(MGMT_CONTEXT) apply -f -

.PHONY: mgmt-cluster-create
mgmt-cluster-create: ## Create Kind management cluster with CAPA provider
	$(SCRIPTS_DIR)/create-aws-mgmt-cluster.sh

.PHONY: mgmt-cluster-delete
mgmt-cluster-delete: ## Delete Kind management cluster
	kind delete cluster --name capi-bench

.PHONY: workload-create
workload-create: ## Create AWS workload cluster and install addons (CNI + CCM)
	@: "$${AWS_SSH_KEY_NAME:?set it to the EC2 key pair for bench nodes}"
	envsubst '$${AWS_SSH_KEY_NAME}' < $(MANIFESTS_DIR)/capa-workload-cluster.yaml | kubectl --context $(MGMT_CONTEXT) apply -f -
	@echo "Waiting for AWS infrastructure to be ready..."
	kubectl --context $(MGMT_CONTEXT) wait --for=jsonpath='{.status.ready}'=true awscluster/$(WORKLOAD_CLUSTER) -n default --timeout=600s
	@echo "Waiting for kubeconfig secret to be created..."
	@for i in $$(seq 1 60); do \
		kubectl --context $(MGMT_CONTEXT) get secret $(WORKLOAD_CLUSTER)-kubeconfig -n default &>/dev/null && break; \
		echo "  Attempt $$i/60..."; \
		sleep 10; \
	done
	$(SCRIPTS_DIR)/install-workload-addons.sh

.PHONY: workload-delete
workload-delete: ## Delete AWS workload cluster
	kubectl --context $(MGMT_CONTEXT) delete -f $(MANIFESTS_DIR)/capa-workload-cluster.yaml --ignore-not-found

.PHONY: workload-reset
workload-reset: $(VENV_PYTHON) ## Scale all MachineDeployments to 0
	cd $(HARNESS_DIR) && $(abspath $(VENV_PYTHON)) -m benchmark.cli reset

.PHONY: eks-create
eks-create: ## Create EKS cluster with Karpenter
	$(SCRIPTS_DIR)/create-eks-cluster.sh

.PHONY: eks-delete
eks-delete: ## Delete EKS cluster
	@: "$${BENCH_AWS_PROFILE:?set it to the aws-vault profile for the bench AWS account}"
	aws-vault exec $(BENCH_AWS_PROFILE) -- eksctl delete cluster --name bench-karpenter --region us-east-1 --wait

# ─── capi-autoscaler deployment ──────────────────────────────────────────────

AUTOSCALER_DIR := $(shell cd .. && pwd)/autoscaler

.PHONY: deploy-capi-autoscaler
deploy-capi-autoscaler: ## Build and deploy capi-autoscaler controller into Kind
	@echo "Building capi-autoscaler binary..."
	cd $(AUTOSCALER_DIR) && cargo build --release
	@echo "Building Docker image..."
	cd $(AUTOSCALER_DIR) && docker build -t capi-autoscaler:bench .
	@echo "Loading image into Kind..."
	kind load docker-image capi-autoscaler:bench --name capi-bench
	@echo "Generating and applying Autoscale CRD..."
	cd $(AUTOSCALER_DIR) && cargo run --bin crdgen | kubectl --context $(MGMT_CONTEXT) apply -f -
	@echo "Copying workload kubeconfig secret to controller namespace..."
	kubectl --context $(MGMT_CONTEXT) create namespace capi-autoscaler-system --dry-run=client -o yaml | kubectl --context $(MGMT_CONTEXT) apply -f -
	kubectl --context $(MGMT_CONTEXT) get secret $(WORKLOAD_CLUSTER)-kubeconfig -n default -o json \
		| jq '.metadata = {name: .metadata.name, namespace: "capi-autoscaler-system"}' \
		| kubectl --context $(MGMT_CONTEXT) apply -f -
	@echo "Deploying controller..."
	kubectl --context $(MGMT_CONTEXT) apply -f $(MANIFESTS_DIR)/capi-autoscaler-deployment.yaml
	kubectl --context $(MGMT_CONTEXT) -n capi-autoscaler-system rollout restart deployment/capi-autoscaler
	@echo "Waiting for controller to be ready..."
	kubectl --context $(MGMT_CONTEXT) -n capi-autoscaler-system wait --for=condition=available deployment/capi-autoscaler --timeout=120s
	@echo "capi-autoscaler deployed successfully."

# ─── Credentials ─────────────────────────────────────────────────────────────

.PHONY: refresh-creds
refresh-creds: ## Refresh AWS credentials and update CAPA controller secret
	@source $(SCRIPTS_DIR)/refresh-aws-credentials.sh && refresh_aws_credentials && \
	echo "Updating CAPA bootstrap credentials secret..." && \
	kubectl --context $(MGMT_CONTEXT) -n capa-system create secret generic \
		capa-manager-bootstrap-credentials \
		--from-literal=credentials="$$(echo $$AWS_B64ENCODED_CREDENTIALS | base64 -d)" \
		--dry-run=client -o yaml | kubectl --context $(MGMT_CONTEXT) apply -f - && \
	echo "Restarting CAPA controller to pick up new credentials..." && \
	kubectl --context $(MGMT_CONTEXT) -n capa-system rollout restart deployment/capa-controller-manager && \
	kubectl --context $(MGMT_CONTEXT) -n capa-system wait --for=condition=Available \
		deployment/capa-controller-manager --timeout=120s && \
	echo "CAPA credentials refreshed."

# ─── Python venv ─────────────────────────────────────────────────────────────

$(VENV_PYTHON):
	python3 -m venv $(HARNESS_DIR)/.venv
	$(HARNESS_DIR)/.venv/bin/pip install -e $(HARNESS_DIR)

.PHONY: venv
venv: $(VENV_PYTHON) ## Create Python venv and install harness dependencies

# ─── Benchmarks ──────────────────────────────────────────────────────────────

.PHONY: bench-capi
bench-capi: $(VENV_PYTHON) refresh-creds workload-create deploy-capi-autoscaler ## Build, deploy, and benchmark capi-autoscaler
	cd $(HARNESS_DIR) && $(abspath $(VENV_PYTHON)) -m benchmark.cli run --autoscaler capi-autoscaler --profile $(PROFILE) --runs $(RUNS) $(PODS_FLAG)
	$(if $(filter 1,$(TEARDOWN)),$(MAKE) workload-delete)

.PHONY: bench-cas
bench-cas: $(VENV_PYTHON) refresh-creds workload-create ## Run benchmarks with Cluster Autoscaler
	cd $(HARNESS_DIR) && $(abspath $(VENV_PYTHON)) -m benchmark.cli run --autoscaler cas --profile $(PROFILE) --runs $(RUNS) $(PODS_FLAG)
	$(if $(filter 1,$(TEARDOWN)),$(MAKE) workload-delete)

.PHONY: bench-karpenter
bench-karpenter: $(VENV_PYTHON) kubeconfig eks-create ## Run benchmarks with Karpenter
	cd $(HARNESS_DIR) && aws-vault exec $(BENCH_AWS_PROFILE) -- $(abspath $(VENV_PYTHON)) -m benchmark.cli run --autoscaler karpenter --profile $(PROFILE) --runs $(RUNS) $(PODS_FLAG)
	$(if $(filter 1,$(TEARDOWN)),$(MAKE) eks-delete)

.PHONY: bench-karpenter-consolidation
bench-karpenter-consolidation: $(VENV_PYTHON) kubeconfig eks-create ## Run benchmarks with Karpenter consolidation enabled (full-window runs)
	cd $(HARNESS_DIR) && aws-vault exec $(BENCH_AWS_PROFILE) -- $(abspath $(VENV_PYTHON)) -m benchmark.cli run --autoscaler karpenter --consolidation --profile $(PROFILE) --runs $(RUNS) $(PODS_FLAG)
	$(if $(filter 1,$(TEARDOWN)),$(MAKE) eks-delete)

.PHONY: bench-all
bench-all: ## Run benchmarks for all autoscalers, sharing one workload cluster
	$(MAKE) bench-capi TEARDOWN=0
	$(MAKE) bench-cas
	$(MAKE) bench-karpenter

# ─── Offline solver benchmark ────────────────────────────────────────────────

SOLVER_BENCH_DIR := solver-bench
SOLVER_BENCH_BIN := $(SOLVER_BENCH_DIR)/target/release/solver-bench
SB_TS := $(shell date +%Y%m%d_%H%M%S)
SB_REPS ?= 3

.PHONY: solver-bench-build
solver-bench-build: ## Build the offline solver benchmark
	cd $(SOLVER_BENCH_DIR) && cargo build --release

.PHONY: solver-bench-rq2
solver-bench-rq2: solver-bench-build ## RQ2 factorial sweep (pods x MDs x slots) -> results/solverbench_*_rq2.csv
	$(SOLVER_BENCH_BIN) rq2 --out results/solverbench_$(SB_TS)_rq2.csv --reps $(SB_REPS)

.PHONY: solver-bench-rq3
solver-bench-rq3: solver-bench-build ## RQ3 beam vs CP-SAT budget ladder -> results/solverbench_*_rq3.csv
	$(SOLVER_BENCH_BIN) rq3 --out results/solverbench_$(SB_TS)_rq3.csv --reps $(SB_REPS)

.PHONY: solver-bench-rq4
solver-bench-rq4: solver-bench-build ## RQ4 consolidation headroom: heavy-constrained-100 at 30s vs 600s budget -> results/solverbench_*_rq4.csv
	$(SOLVER_BENCH_BIN) rq4 --out results/solverbench_$(SB_TS)_rq4.csv --reps $(SB_REPS)

.PHONY: solver-bench-all
solver-bench-all: solver-bench-rq3 solver-bench-rq2 ## Full offline sweep

# ─── Cleanup ─────────────────────────────────────────────────────────────────

.PHONY: cleanup
cleanup: ## Clean up all benchmark AWS resources
	$(SCRIPTS_DIR)/cleanup-all.sh
