#!/usr/bin/env bash
# Clean up all benchmark AWS resources
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-capi-bench}"
EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME:-bench-karpenter}"
AWS_REGION="${AWS_REGION:-us-east-1}"
WORKLOAD_CLUSTER_NAME="${WORKLOAD_CLUSTER_NAME:-bench-cluster}"

source "${SCRIPT_DIR}/refresh-aws-credentials.sh"
refresh_aws_credentials

echo "=== Benchmark Resource Cleanup ==="
echo ""

# 1. Delete workload cluster (CAPA will deprovision EC2 instances)
echo "--- Deleting CAPA workload cluster ---"
if kubectl --context "kind-${KIND_CLUSTER_NAME}" get cluster "${WORKLOAD_CLUSTER_NAME}" -n default &>/dev/null; then
  kubectl --context "kind-${KIND_CLUSTER_NAME}" delete cluster "${WORKLOAD_CLUSTER_NAME}" -n default --wait=false
  echo "Waiting for CAPA to deprovision workload cluster EC2 instances..."
  kubectl --context "kind-${KIND_CLUSTER_NAME}" wait --for=delete \
    "cluster/${WORKLOAD_CLUSTER_NAME}" -n default --timeout=600s 2>/dev/null || true
  echo "Workload cluster deletion initiated (may take several minutes for EC2 cleanup)"
else
  echo "Workload cluster '${WORKLOAD_CLUSTER_NAME}' not found, skipping"
fi

# 2. Delete EKS cluster
echo ""
echo "--- Deleting EKS cluster ---"
if aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    aws eks describe-cluster --name "${EKS_CLUSTER_NAME}" --region "${AWS_REGION}" &>/dev/null; then
  # Terminate any Karpenter-launched instances first
  KARPENTER_INSTANCES=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    aws ec2 describe-instances --region "${AWS_REGION}" \
      --filters "Name=tag:karpenter.sh/nodepool,Values=*" "Name=instance-state-name,Values=running,pending" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || echo "")
  if [[ -n "${KARPENTER_INSTANCES}" && "${KARPENTER_INSTANCES}" != "None" ]]; then
    echo "  Terminating Karpenter instances: ${KARPENTER_INSTANCES}"
    aws-vault exec "${BENCH_AWS_PROFILE}" -- \
      aws ec2 terminate-instances --instance-ids ${KARPENTER_INSTANCES} --region "${AWS_REGION}" >/dev/null
    aws-vault exec "${BENCH_AWS_PROFILE}" -- \
      aws ec2 wait instance-terminated --instance-ids ${KARPENTER_INSTANCES} --region "${AWS_REGION}" 2>/dev/null || true
  fi

  # Get VPC ID before deleting cluster (needed for SG cleanup)
  EKS_VPC_ID=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    aws eks describe-cluster --name "${EKS_CLUSTER_NAME}" --region "${AWS_REGION}" \
    --query 'cluster.resourcesVpcConfig.vpcId' --output text 2>/dev/null || echo "")

  echo "Deleting EKS cluster '${EKS_CLUSTER_NAME}'..."
  aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    eksctl delete cluster --name "${EKS_CLUSTER_NAME}" --region "${AWS_REGION}" --wait || {
    # If eksctl delete fails, clean up orphaned security groups and retry
    if [[ -n "${EKS_VPC_ID}" && "${EKS_VPC_ID}" != "None" ]]; then
      echo "  Cleaning up orphaned security groups in VPC ${EKS_VPC_ID}..."
      SG_IDS=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
        aws ec2 describe-security-groups --region "${AWS_REGION}" \
        --filters "Name=vpc-id,Values=${EKS_VPC_ID}" \
        --query 'SecurityGroups[?GroupName!=`default`].GroupId' --output text 2>/dev/null || echo "")
      for sg in ${SG_IDS}; do
        aws-vault exec "${BENCH_AWS_PROFILE}" -- \
          aws ec2 delete-security-group --group-id "${sg}" --region "${AWS_REGION}" 2>/dev/null || true
      done
    fi
    # Retry stack deletion
    aws-vault exec "${BENCH_AWS_PROFILE}" -- \
      aws cloudformation delete-stack --stack-name "eksctl-${EKS_CLUSTER_NAME}-cluster" --region "${AWS_REGION}" 2>/dev/null || true
    aws-vault exec "${BENCH_AWS_PROFILE}" -- \
      aws cloudformation wait stack-delete-complete --stack-name "eksctl-${EKS_CLUSTER_NAME}-cluster" --region "${AWS_REGION}" 2>/dev/null || true
  }
else
  echo "EKS cluster '${EKS_CLUSTER_NAME}' not found, skipping"
fi

# 3. Delete Karpenter CloudFormation stack
KARPENTER_CFN_STACK="Karpenter-${EKS_CLUSTER_NAME}"
echo ""
echo "--- Deleting Karpenter CloudFormation stack ---"
if aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    aws cloudformation describe-stacks --stack-name "${KARPENTER_CFN_STACK}" --region "${AWS_REGION}" &>/dev/null; then
  # Clean up Karpenter instance profiles that block IAM role deletion
  KARPENTER_NODE_ROLE="KarpenterNodeRole-${EKS_CLUSTER_NAME}"
  IPS=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    aws iam list-instance-profiles-for-role --role-name "${KARPENTER_NODE_ROLE}" \
    --query 'InstanceProfiles[].InstanceProfileName' --output text 2>/dev/null || echo "")
  for ip in ${IPS}; do
    echo "  Removing instance profile ${ip}..."
    aws-vault exec "${BENCH_AWS_PROFILE}" -- \
      aws iam remove-role-from-instance-profile \
        --instance-profile-name "${ip}" --role-name "${KARPENTER_NODE_ROLE}" 2>/dev/null || true
    aws-vault exec "${BENCH_AWS_PROFILE}" -- \
      aws iam delete-instance-profile --instance-profile-name "${ip}" 2>/dev/null || true
  done

  echo "Deleting CloudFormation stack '${KARPENTER_CFN_STACK}'..."
  aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    aws cloudformation delete-stack --stack-name "${KARPENTER_CFN_STACK}" --region "${AWS_REGION}"
  aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    aws cloudformation wait stack-delete-complete --stack-name "${KARPENTER_CFN_STACK}" --region "${AWS_REGION}" 2>/dev/null || true
  echo "CloudFormation stack deleted"
else
  echo "Karpenter CloudFormation stack not found, skipping"
fi

# 4. Delete Kind management cluster
echo ""
echo "--- Deleting Kind management cluster ---"
if kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
  kind delete cluster --name "${KIND_CLUSTER_NAME}"
  echo "Kind cluster '${KIND_CLUSTER_NAME}' deleted"
else
  echo "Kind cluster '${KIND_CLUSTER_NAME}' not found, skipping"
fi

# 5. Safety check: look for orphaned EC2 instances
echo ""
echo "--- Checking for orphaned EC2 instances ---"
ORPHANS=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  aws ec2 describe-instances \
    --region "${AWS_REGION}" \
    --filters \
      "Name=tag:sigs.k8s.io/cluster-api-provider-aws/cluster/${WORKLOAD_CLUSTER_NAME},Values=owned" \
      "Name=instance-state-name,Values=running,pending,stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' \
    --output text 2>/dev/null || echo "")

if [[ -n "${ORPHANS}" && "${ORPHANS}" != "None" ]]; then
  echo "WARNING: Found orphaned EC2 instances with CAPA tags:"
  echo "  ${ORPHANS}"
  echo ""
  echo "To terminate these manually:"
  echo "  aws-vault exec ${BENCH_AWS_PROFILE} -- aws ec2 terminate-instances --instance-ids ${ORPHANS} --region ${AWS_REGION}"
else
  echo "No orphaned EC2 instances found."
fi

echo ""
echo "=== Cleanup complete ==="
