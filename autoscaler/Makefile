.PHONY: dev dev-up dev-down build test clean cluster-create cluster-delete \
       apply-test-cluster install-crd install-cni wait-for-workload-cluster \
       demo-apply demo-watch demo-cleanup

CLUSTER_NAME ?= test-cluster
NAMESPACE ?= default
KIND_CLUSTER_NAME ?= capi-test
WORKLOAD_KUBECONFIG ?= /tmp/$(CLUSTER_NAME)-kubeconfig

# Full development environment: KIND cluster + CAPI + Tilt
dev:
	./scripts/create-kind-mgmt-cluster.sh

dev-up:
	tilt up

dev-down:
	tilt down

# Build
build:
	cargo build --release

test:
	cargo test

clean:
	cargo clean

# KIND cluster management (without starting Tilt)
cluster-create:
	@if docker ps -q --filter "name=$(CLUSTER_NAME)-" 2>/dev/null | grep -q .; then \
		echo "Cleaning up orphaned CAPD containers from a previous run..."; \
		docker ps -aq --filter "name=$(CLUSTER_NAME)-" | xargs -r docker rm -f; \
	fi
	@KIND_CLUSTER_NAME=$(KIND_CLUSTER_NAME) NO_TILT=1 ./scripts/create-kind-mgmt-cluster.sh

cluster-delete:
	@echo "Removing CAPD workload cluster containers..."
	@docker ps -aq --filter "name=$(CLUSTER_NAME)-" | xargs -r docker rm -f 2>/dev/null || true
	@rm -f $(WORKLOAD_KUBECONFIG)
	kind delete cluster --name $(KIND_CLUSTER_NAME)

# Install CRD
install-crd:
	cargo run --bin crdgen 2>/dev/null | kubectl apply -f -

# Test resources
apply-test-cluster:
	kubectl apply -f manifests/test-cluster.yaml
	kubectl apply -f manifests/autoscale.yaml

delete-test-cluster:
	kubectl delete -f manifests/test-cluster.yaml --ignore-not-found

# Wait for workload cluster kubeconfig and control plane
wait-for-workload-cluster:
	@echo "Waiting for $(CLUSTER_NAME) kubeconfig secret..."
	kubectl wait --for=create secret/$(CLUSTER_NAME)-kubeconfig -n $(NAMESPACE) --timeout=600s
	@echo "Extracting workload kubeconfig to $(WORKLOAD_KUBECONFIG)..."
	kubectl get secret $(CLUSTER_NAME)-kubeconfig -n $(NAMESPACE) \
		-o jsonpath='{.data.value}' | base64 -d > $(WORKLOAD_KUBECONFIG)
	@echo "Waiting for control plane node to appear..."
	@until kubectl --kubeconfig $(WORKLOAD_KUBECONFIG) get nodes 2>/dev/null | grep -q control-plane; do \
		sleep 5; echo "  still waiting..."; \
	done
	@echo "Workload cluster control plane is up."

# Install CNI (kindnet) on the workload cluster so nodes become Ready
install-cni:
	@echo "Installing kindnet CNI on workload cluster..."
	kubectl --kubeconfig $(WORKLOAD_KUBECONFIG) apply -f \
		https://raw.githubusercontent.com/aojea/kindnet/main/install-kindnet.yaml
	@echo "Waiting for nodes to become Ready..."
	kubectl --kubeconfig $(WORKLOAD_KUBECONFIG) wait --for=condition=Ready nodes --all --timeout=300s
	@echo "All workload cluster nodes are Ready."

# Demo workload (for IDV demonstration video)
demo-apply:
	kubectl --kubeconfig $(WORKLOAD_KUBECONFIG) apply -f ../benchmarks/manifests/demo-workload.yaml
	@echo "Workload deployed. Watch pods with: make demo-watch"

demo-watch:
	kubectl --kubeconfig $(WORKLOAD_KUBECONFIG) get pods -w

demo-cleanup:
	kubectl --kubeconfig $(WORKLOAD_KUBECONFIG) delete -f ../benchmarks/manifests/demo-workload.yaml --ignore-not-found

# Generate CRDs
generate-crds:
	cargo run --bin crdgen > crds/autoscaler-crds.yaml

# Quick status check
status:
	@echo "=== Clusters ==="
	@kubectl get clusters -A 2>/dev/null || echo "No clusters found"
	@echo ""
	@echo "=== MachineDeployments ==="
	@kubectl get machinedeployments -A 2>/dev/null || echo "No machinedeployments found"
	@echo ""
	@echo "=== Machines ==="
	@kubectl get machines -A 2>/dev/null || echo "No machines found"

# Help
help:
	@echo "Available targets:"
	@echo "  dev              - Full setup: KIND cluster + CAPI providers + Tilt"
	@echo "  dev-up           - Start Tilt (assumes cluster exists)"
	@echo "  dev-down         - Stop Tilt"
	@echo "  build            - Build the autoscaler binary"
	@echo "  test             - Run tests"
	@echo "  clean            - Clean build artifacts"
	@echo "  cluster-create   - Create KIND cluster with CAPI providers (no Tilt)"
	@echo "  cluster-delete   - Delete KIND cluster"
	@echo "  install-crd      - Install the Autoscale CRD"
	@echo "  apply-test-cluster   - Deploy test cluster + autoscale manifests"
	@echo "  wait-for-workload-cluster - Wait for workload cluster to be reachable"
	@echo "  install-cni      - Install kindnet CNI on the workload cluster"
	@echo "  demo-apply       - Deploy demo workload (triggers scale-up)"
	@echo "  demo-watch       - Watch workload pods in the workload cluster"
	@echo "  demo-cleanup     - Remove demo workload from workload cluster"
	@echo "  status           - Show CAPI resources status"
	@echo "  help             - Show this help"
