#!/usr/bin/env bash
# Refresh AWS credentials for CAPA benchmarking via aws-vault
set -euo pipefail

BENCH_AWS_PROFILE="${BENCH_AWS_PROFILE:?set it to the aws-vault profile for the bench AWS account}"
AWS_REGION="${AWS_REGION:-us-east-1}"

refresh_aws_credentials() {
  # Unset AWS_VAULT to avoid interference when already inside an aws-vault session
  unset AWS_VAULT 2>/dev/null || true

  echo "Encoding AWS credentials for CAPA..."
  export AWS_B64ENCODED_CREDENTIALS
  AWS_B64ENCODED_CREDENTIALS=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- \
    clusterawsadm bootstrap credentials encode-as-profile --region "${AWS_REGION}")

  # Also export raw credentials for tools that need them directly
  local env
  env=$(aws-vault exec "${BENCH_AWS_PROFILE}" -- env | grep -E '(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN)')
  eval "export ${env}"

  echo "AWS credentials refreshed for profile: ${BENCH_AWS_PROFILE}"
}

# When sourced, export the function. When executed directly, run it.
if [[ "${BASH_SOURCE[0]:-$0}" == "${0}" ]]; then
  refresh_aws_credentials
fi
