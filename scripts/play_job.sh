#!/usr/bin/env bash

set -eo pipefail

# This script takes a Kubernetes Job manifest as its sole input and plays the checker locally with same settings.
# https://github.com/kislyuk/yq is required to be available
# It expects to be able to connect to Kubernetes to grab its configmap (so kubectx must be correct, VPN up, and AWS SSO on).
# Namespace defaults to "pulse" but can be overriden with $NAMESPACE

# Basic checks
JOB_MANIFEST_FILE="$1"
NAMESPACE="${NAMESPACE:-pulse}"

[[ -z "${JOB_MANIFEST_FILE}" ]] && {
  echo "Give a checker Job manifest as first argument (similar to what scripts/prepare.py makes)"
  exit 1
}

[[ ! -f "${JOB_MANIFEST_FILE}" ]] && {
  echo "File not found: $JOB_MANIFEST_FILE"
  exit 1
}

# Load configmap from cluster
_configmap_name=$(yq -r 'select(has("spec")) | .spec.template.spec.volumes[] | select(.name == "config-volume") | objects | .configMap.name' "${JOB_MANIFEST_FILE}")


# Save configmap to tempfile
trap 'rm -f "_configmap_path"' EXIT INT TERM HUP QUIT ABRT PIPE
_configmap_path=$(mktemp consolidator_checker_config_XXXXXX.yaml)
kubectl -n "${NAMESPACE}" get -o yaml configmap "${_configmap_name}" | yq -r '.data["config.yaml"]' > "$_configmap_path"

# Reset environment
for variable_name in $(env | grep CONSOLIDATOR); do
  unset "$variable_name"
done

# Extract and set environment variables from job manifest
_job_env=$(yq -r 'select(has("spec")) | .spec.template.spec.containers[0].env.[] | "export \(.name)=\(.value)"' "${JOB_MANIFEST_FILE}")
eval "$_job_env"
export LOG_LEVEL=trace
export LOG_CONSOLIDATOR_CHECKER_CONFIG_FILE="$_configmap_path"

# Run job
_SCRIPT_LOCATION=$(realpath "${BASH_SOURCE[0]}")
_REPO_ROOT=$(realpath "$(dirname "${_SCRIPT_LOCATION}")/..")
tsx "${_REPO_ROOT}/src/commands/check.ts"
