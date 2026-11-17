#!/usr/bin/env bash
# Install required addons (CNI + AWS CCM) on the workload cluster
# Must be run after the workload cluster control plane is available
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
MGMT_CONTEXT="${MGMT_CONTEXT:-kind-capi-bench}"
WORKLOAD_CLUSTER="${WORKLOAD_CLUSTER:-bench-cluster}"
KUBECONFIG_SECRET="${WORKLOAD_CLUSTER}-kubeconfig"

echo "Extracting workload cluster kubeconfig..."
WORKLOAD_KUBECONFIG=$(mktemp /tmp/bench-kubeconfig.XXXXXX)
kubectl --context "${MGMT_CONTEXT}" get secret "${KUBECONFIG_SECRET}" -n default \
  -o jsonpath='{.data.value}' | base64 -d > "${WORKLOAD_KUBECONFIG}"
trap "rm -f ${WORKLOAD_KUBECONFIG}" EXIT

echo "Waiting for workload cluster API server to be reachable..."
for i in $(seq 1 60); do
  if kubectl --kubeconfig "${WORKLOAD_KUBECONFIG}" get nodes &>/dev/null; then
    break
  fi
  echo "  Attempt ${i}/60..."
  sleep 5
done

echo "Installing Calico CNI (VXLAN mode - BGP mesh does not converge reliably during mass scale-up)..."
kubectl --kubeconfig "${WORKLOAD_KUBECONFIG}" apply \
  -f "${SCRIPT_DIR}/../manifests/workload-addons/calico-vxlan.yaml" 2>&1 | tail -3

echo "Installing AWS Cloud Controller Manager..."
kubectl --kubeconfig "${WORKLOAD_KUBECONFIG}" apply \
  -f "${SCRIPT_DIR}/../manifests/workload-addons/aws-ccm.yaml"

echo "Waiting for CCM to be running..."
kubectl --kubeconfig "${WORKLOAD_KUBECONFIG}" rollout status \
  daemonset/aws-cloud-controller-manager -n kube-system --timeout=120s

echo "Waiting for control plane node to be Ready..."
kubectl --kubeconfig "${WORKLOAD_KUBECONFIG}" wait --for=condition=Ready node --all --timeout=300s

echo "Workload cluster addons installed successfully."
