#!/usr/bin/env bash
# Create an EKS cluster with Karpenter for benchmarking
# Follows the official Karpenter getting-started guide for v1.1+
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME:-bench-karpenter}"
AWS_REGION="${AWS_REGION:-us-east-1}"
K8S_VERSION="${K8S_VERSION:-1.35}"
KARPENTER_VERSION="${KARPENTER_VERSION:-1.14.1}"
KARPENTER_NAMESPACE="kube-system"
AWS_PARTITION="aws"

# Validate required commands
for cmd in eksctl helm aws-vault kubectl; do
  if ! command -v "${cmd}" &>/dev/null; then
    echo "ERROR: ${cmd} is required but not found in PATH"
    exit 1
  fi
done

# Source credential helper
source "${SCRIPT_DIR}/refresh-aws-credentials.sh"
refresh_aws_credentials

AWS_ACCOUNT_ID=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  aws sts get-caller-identity --query Account --output text)
echo "AWS Account ID: ${AWS_ACCOUNT_ID}"

# Step 1: Deploy Karpenter CloudFormation stack (IAM roles + policies)
# Always deploy: eks-delete leaves the stack behind, and an old stack carries
# an old controller policy. Deploy is a no-op when nothing changed.
KARPENTER_CFN_STACK="Karpenter-${EKS_CLUSTER_NAME}"
echo "Deploying Karpenter IAM resources via CloudFormation..."
TEMPOUT="$(mktemp)"
curl -fsSL "https://raw.githubusercontent.com/aws/karpenter-provider-aws/v${KARPENTER_VERSION}/website/content/en/preview/getting-started/getting-started-with-karpenter/cloudformation.yaml" > "${TEMPOUT}"
aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  aws cloudformation deploy \
    --stack-name "${KARPENTER_CFN_STACK}" \
    --region "${AWS_REGION}" \
    --template-file "${TEMPOUT}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --no-fail-on-empty-changeset \
    --parameter-overrides "ClusterName=${EKS_CLUSTER_NAME}"
rm -f "${TEMPOUT}"

# Step 2: Create EKS cluster with eksctl
if aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    aws eks describe-cluster --name "${EKS_CLUSTER_NAME}" --region "${AWS_REGION}" &>/dev/null; then
  echo "EKS cluster '${EKS_CLUSTER_NAME}' already exists, skipping creation"
else
  echo "Creating EKS cluster '${EKS_CLUSTER_NAME}'..."
  cat <<EOF | aws-vault exec "${BENCH_AWS_PROFILE}" -- eksctl create cluster -f -
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: ${EKS_CLUSTER_NAME}
  region: ${AWS_REGION}
  version: "${K8S_VERSION}"
  tags:
    karpenter.sh/discovery: ${EKS_CLUSTER_NAME}

# Match the CAPA workload cluster's 3 zones so spread scenarios are equivalent
availabilityZones: ["us-east-1a", "us-east-1b", "us-east-1c"]

iam:
  withOIDC: true
  podIdentityAssociations:
    - namespace: ${KARPENTER_NAMESPACE}
      serviceAccountName: karpenter
      roleName: ${EKS_CLUSTER_NAME}-karpenter
      # Karpenter 1.14 split KarpenterControllerPolicy into six scoped policies
      permissionPolicyARNs:
        - arn:${AWS_PARTITION}:iam::${AWS_ACCOUNT_ID}:policy/KarpenterControllerEKSIntegrationPolicy-${EKS_CLUSTER_NAME}
        - arn:${AWS_PARTITION}:iam::${AWS_ACCOUNT_ID}:policy/KarpenterControllerIAMIntegrationPolicy-${EKS_CLUSTER_NAME}
        - arn:${AWS_PARTITION}:iam::${AWS_ACCOUNT_ID}:policy/KarpenterControllerInterruptionPolicy-${EKS_CLUSTER_NAME}
        - arn:${AWS_PARTITION}:iam::${AWS_ACCOUNT_ID}:policy/KarpenterControllerNodeLifecyclePolicy-${EKS_CLUSTER_NAME}
        - arn:${AWS_PARTITION}:iam::${AWS_ACCOUNT_ID}:policy/KarpenterControllerResourceDiscoveryPolicy-${EKS_CLUSTER_NAME}
        - arn:${AWS_PARTITION}:iam::${AWS_ACCOUNT_ID}:policy/KarpenterControllerZonalShiftPolicy-${EKS_CLUSTER_NAME}

iamIdentityMappings:
  - arn: arn:${AWS_PARTITION}:iam::${AWS_ACCOUNT_ID}:role/KarpenterNodeRole-${EKS_CLUSTER_NAME}
    username: system:node:{{EC2PrivateDNSName}}
    groups:
      - system:bootstrappers
      - system:nodes

managedNodeGroups:
  - instanceType: m5.large
    amiFamily: AmazonLinux2023
    name: system
    desiredCapacity: 2
    minSize: 2
    maxSize: 2
    taints:
      - key: CriticalAddonsOnly
        value: "true"
        effect: NoSchedule

addons:
  - name: eks-pod-identity-agent
EOF
fi

# Step 3: Ensure kubeconfig is set
aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  eksctl utils write-kubeconfig --cluster "${EKS_CLUSTER_NAME}" --region "${AWS_REGION}"

# Step 4: Tag subnets for Karpenter discovery
echo "Tagging subnets for Karpenter discovery..."
SUBNET_IDS=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  aws ec2 describe-subnets \
    --region "${AWS_REGION}" \
    --filters "Name=tag:alpha.eksctl.io/cluster-name,Values=${EKS_CLUSTER_NAME}" \
    --query 'Subnets[].SubnetId' --output text)

for subnet_id in ${SUBNET_IDS}; do
  aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    aws ec2 create-tags \
      --region "${AWS_REGION}" \
      --resources "${subnet_id}" \
      --tags "Key=karpenter.sh/discovery,Value=${EKS_CLUSTER_NAME}" 2>/dev/null || true
done

# Step 5: Tag cluster security group for Karpenter discovery
echo "Tagging security groups for Karpenter discovery..."
CLUSTER_SG=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  aws eks describe-cluster \
    --name "${EKS_CLUSTER_NAME}" \
    --region "${AWS_REGION}" \
    --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' --output text)

aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  aws ec2 create-tags \
    --region "${AWS_REGION}" \
    --resources "${CLUSTER_SG}" \
    --tags "Key=karpenter.sh/discovery,Value=${EKS_CLUSTER_NAME}" 2>/dev/null || true

# Step 6: Install Karpenter via Helm
echo "Installing Karpenter ${KARPENTER_VERSION}..."
CLUSTER_ENDPOINT=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  aws eks describe-cluster --name "${EKS_CLUSTER_NAME}" --region "${AWS_REGION}" \
  --query 'cluster.endpoint' --output text)

helm registry logout public.ecr.aws 2>/dev/null || true
aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  helm upgrade --install karpenter oci://public.ecr.aws/karpenter/karpenter \
    --version "${KARPENTER_VERSION}" \
    --namespace "${KARPENTER_NAMESPACE}" \
    --set "settings.clusterName=${EKS_CLUSTER_NAME}" \
    --set "settings.clusterEndpoint=${CLUSTER_ENDPOINT}" \
    --set "settings.interruptionQueue=" \
    --set "settings.featureGates.nodeOverlay=true" \
    --set controller.resources.requests.cpu=1 \
    --set controller.resources.requests.memory=1Gi \
    --set controller.resources.limits.cpu=1 \
    --set controller.resources.limits.memory=1Gi \
    --set "tolerations[0].key=CriticalAddonsOnly" \
    --set "tolerations[0].operator=Exists" \
    --set "tolerations[0].effect=NoSchedule" \
    --wait

# Step 7: Apply NodePool and EC2NodeClass
echo "Applying Karpenter NodePool manifests..."
aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  kubectl apply -f "${SCRIPT_DIR}/../manifests/karpenter/node-pool.yaml"
aws-vault exec "${BENCH_AWS_PROFILE}" -- \
  kubectl apply -f "${SCRIPT_DIR}/../manifests/karpenter/node-overlay.yaml"

echo ""
echo "EKS cluster '${EKS_CLUSTER_NAME}' is ready with Karpenter ${KARPENTER_VERSION}."
echo "System node group: 2x m5.large"
echo "Karpenter NodePool and EC2NodeClass applied."
