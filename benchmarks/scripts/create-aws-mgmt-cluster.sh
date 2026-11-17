#!/usr/bin/env bash
# Create a Kind management cluster with CAPA (AWS) infrastructure provider for benchmarking
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-capi-bench}"
KIND_KUBE_CONTEXT="kind-${KIND_CLUSTER_NAME}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Validate required commands
for cmd in kind kubectl clusterctl clusterawsadm aws-vault; do
  if ! command -v "${cmd}" &>/dev/null; then
    echo "ERROR: ${cmd} is required but not found in PATH"
    exit 1
  fi
done

# Source credential helper
source "${SCRIPT_DIR}/refresh-aws-credentials.sh"
refresh_aws_credentials

# Create Kind cluster if it doesn't exist
if kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
  echo "Kind cluster '${KIND_CLUSTER_NAME}' already exists, skipping creation"
else
  echo "Creating Kind cluster '${KIND_CLUSTER_NAME}'..."
  kind create cluster --name "${KIND_CLUSTER_NAME}" --wait 60s
fi

# Ensure we're using the right context
kubectl config use-context "${KIND_KUBE_CONTEXT}"

# Initialize CAPI with AWS infrastructure provider
echo "Initializing Cluster API with AWS provider..."
export AWS_B64ENCODED_CREDENTIALS
export EXP_MACHINE_POOL=true

clusterctl init \
  --infrastructure aws \
  --wait-providers \
  --wait-provider-timeout 300

# Wait for CAPA controllers to be ready
echo "Waiting for CAPA controllers..."
kubectl wait --for=condition=Available deployment/capa-controller-manager \
  -n capa-system --timeout=300s

echo "Waiting for CAPI core controllers..."
kubectl wait --for=condition=Available deployment/capi-controller-manager \
  -n capi-system --timeout=300s

echo ""
echo "Management cluster '${KIND_CLUSTER_NAME}' is ready with CAPA provider."
echo "Context: ${KIND_KUBE_CONTEXT}"
echo ""
echo "Next steps:"
echo "  make -C benchmarks workload-create"
