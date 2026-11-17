#!/usr/bin/env bash

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}/.." || exit

KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:-capi-test}
KIND_KUBE_CONTEXT="kind-${KIND_CLUSTER_NAME}"

# Versions - update as needed
CAPI_VERSION=${CAPI_VERSION:-v1.13.2}
CAPD_VERSION=${CAPD_VERSION:-v1.13.2}

main() {
  ensure_command kind
  ensure_command kubectl
  ensure_command clusterctl
  ensure_command tilt

  ensure_kind_cluster
  ensure_capi_providers

  echo ""
  echo "Management cluster ready!"
  echo "  Context: ${KIND_KUBE_CONTEXT}"

  if [ "${NO_TILT:-}" = "1" ]; then
    echo ""
    echo "Skipping Tilt (NO_TILT=1). Run 'tilt up' or 'make dev-up' to start."
  else
    echo ""
    echo "Starting Tilt..."
    tilt up
  fi
}

ensure_kind_cluster() {
  if kind get clusters 2>/dev/null | grep -q "^${KIND_CLUSTER_NAME}$"; then
    echo "KIND cluster '${KIND_CLUSTER_NAME}' already exists"
    kubectl config use-context "${KIND_KUBE_CONTEXT}"
    return
  fi

  echo "Creating KIND cluster '${KIND_CLUSTER_NAME}'..."

  # Create cluster with extra port mappings for the Docker provider
  cat <<EOF | kind create cluster --name "${KIND_CLUSTER_NAME}" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraMounts:
    - hostPath: /var/run/docker.sock
      containerPath: /var/run/docker.sock
EOF

  kubectl config use-context "${KIND_KUBE_CONTEXT}"
}

ensure_capi_providers() {
  # Check if CAPI is already installed
  if kubectl get deployment -n capi-system capi-controller-manager &>/dev/null; then
    echo "CAPI providers already installed"
    return
  fi

  echo "Installing CAPI providers..."

  # Initialize with core CAPI, kubeadm bootstrap/control-plane, and Docker infrastructure
  clusterctl init \
    --core "cluster-api:${CAPI_VERSION}" \
    --bootstrap "kubeadm:${CAPI_VERSION}" \
    --control-plane "kubeadm:${CAPI_VERSION}" \
    --infrastructure "docker:${CAPD_VERSION}"

  echo "Waiting for CAPI controllers to be ready..."
  kubectl wait --for=condition=Available --timeout=300s deployment -n capi-system --all
  kubectl wait --for=condition=Available --timeout=300s deployment -n capd-system --all
  kubectl wait --for=condition=Available --timeout=300s deployment -n capi-kubeadm-bootstrap-system --all
  kubectl wait --for=condition=Available --timeout=300s deployment -n capi-kubeadm-control-plane-system --all

  echo "CAPI providers installed successfully"
}

ensure_command() {
  type -P "${1}" &> /dev/null || {
    echo "Command '${1}' not found. Please install it first."
    exit 1
  }
}

main "$@"
