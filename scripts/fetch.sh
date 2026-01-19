#!/usr/bin/env bash
# Try to fetch the latest successful runs of the log-consolidator jobs and create their checker jobs
# Required env var: DD_API_KEY, DD_APP_KEY, FROM
# You need to have generated the job manifests before running this script

if [[ "$(uname)" == 'Darwin' ]]; then
  if ! command -v gdate &>/dev/null; then
    echo "gdate is not installed. Please install it using Homebrew:"
    echo "  brew install coreutils"
    exit 1
  fi
  DATE_CMD="gdate"
else
  DATE_CMD="date"
fi

FROM=${FROM:-$($DATE_CMD -d "2024-10-24" +%s)}
TO=${TO:-$($DATE_CMD -d "now" +%s)}

curl -X POST "https://api.datadoghq.eu/api/v2/logs/events/search" \
  -H "Content-Type: application/json" \
  -H "DD-API-KEY: ${DD_API_KEY}" \
  -H "DD-APPLICATION-KEY: ${DD_APP_KEY}" \
  -d @- >from_dd_logs.json <<EOF
{
    "filter": {
        "from":${FROM},
        "to":${TO}000,
        "query":"service:infra-log-consolidator-* consolidated"
    },
    "page": {
        "limit":1000
    }
}
EOF

_done_jobs=$(jq -r '.data[].attributes.tags[] | select(. | test("pod_name"))' \
  <from_dd_logs.json | sort | uniq)

echo "Found $(echo "$_done_jobs" | wc -l) done jobs"
export IFS=$'\n'
_pattern="zstd-([^-]+)-(........)t([^-]+).*"
for _done_job in $_done_jobs; do
  if [[ $_done_job =~ $_pattern ]]; then
    _level="${BASH_REMATCH[1]}"
    _day="${BASH_REMATCH[2]}"
    _hour="${BASH_REMATCH[3]}"

    echo "kubectl apply -n pulse -f /Users/gabriel.dugny/Sources/pulse/consolidator-checker/infra/k8s/jobs/job_manifest_${_day}${_hour}.yaml"
    kubectl apply -n pulse -f /Users/gabriel.dugny/Sources/pulse/consolidator-checker/infra/k8s/jobs/job_manifest_${_day}${_hour}.yaml
  else
    echo "Problem parsing $_done_job name" >&2
  fi
  # kube_job:log-consolidator-global-zstd-8-mod-8-rem-2-20200903-20200927
done
