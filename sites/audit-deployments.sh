#!/usr/bin/env bash
# Create the deployments the audit flows run behind: one site-audit per site,
# each pinned to that site's pool, plus the fleet-audit orchestrator that fans
# out to all of them. Idempotent: a deployment that already exists is updated in
# place, so re-running after changing a default is the way to roll it out.
#
# Run ./sites/issue-keys.sh first -- it creates the pools these deployments
# target. The site containers need no restart afterwards: a deployment is
# server-side state, and the workers already advertise the flows.
#
#   PRIMEFLOW_URL       server base URL     (default http://localhost:8085)
#   PRIMEFLOW_ADMIN     operator email      (default admin@primeflow.local)
#   PRIMEFLOW_PASSWORD  operator password   (default primeflow-admin)
#   SITES               space-separated     (default "site-a site-b site-c site-d vm1")
#   FLEET_QUEUE         pool the orchestrator itself runs on (default "default")
#
# The orchestrator must not run on a site pool: it waits on its children, and a
# site pool deep enough to hold both parent and children is not something a real
# site VM has.
set -euo pipefail

SERVER="${PRIMEFLOW_URL:-http://localhost:8085}"
EMAIL="${PRIMEFLOW_ADMIN:-admin@primeflow.local}"
PASSWORD="${PRIMEFLOW_PASSWORD:-primeflow-admin}"
SITES="${SITES:-site-a site-b site-c site-d vm1}"
FLEET_QUEUE="${FLEET_QUEUE:-default}"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

JAR="$(mktemp)"
trap 'rm -f "$JAR"' EXIT

curl -sfS -c "$JAR" -X POST "$SERVER/api/v1/auth/login" -H 'Content-Type: application/json' \
  -d "$(jq -cn --arg e "$EMAIL" --arg p "$PASSWORD" '{email:$e,password:$p}')" >/dev/null
CSRF="$(awk '$6=="pf_csrf"{print $7}' "$JAR")"

api() { # method path [json-body]
  curl -sfS -b "$JAR" -X "$1" "$SERVER/api/v1$2" \
    -H 'Content-Type: application/json' -H "X-CSRF-Token: $CSRF" ${3:+-d "$3"}
}

for site in $SITES; do
  # retries 0 is deliberate. site-audit rolls back on failure, which leaves its
  # forward checkpoints COMPLETED for resources that no longer exist; a retry
  # would skip rebuilding them and seal a snapshot it never took. Rolling
  # forward from a rollback is a new run, not a retry.
  api POST /deployments "$(jq -cn --arg s "$site" '{
    name: ("site-audit-" + $s),
    flow_name: "site-audit",
    description: ("Deep audit of " + $s + " — probes, snapshot, saga rollback"),
    parameters: {site: $s, bake_seconds: 8},
    work_queue: $s,
    tags: ["demo", "audit", "site"],
    retries: 0,
    timeout: "15m"
  }')" | jq -r '"\(.name): flow \(.flow_name) on pool \(.work_queue)"'
done

api POST /deployments "$(jq -cn \
  --argjson sites "$(printf '%s\n' $SITES | jq -R . | jq -sc .)" \
  --arg queue "$FLEET_QUEUE" '{
    name: "fleet-audit",
    flow_name: "fleet-audit",
    description: "Audit every site: one canary, then parallel waves of child runs",
    parameters: {sites: $sites, wave_size: 2, bake_seconds: 8},
    work_queue: $queue,
    tags: ["demo", "audit", "fleet"],
    retries: 0,
    timeout: "1h"
  }')" | jq -r '"\(.name): flow \(.flow_name) on pool \(.work_queue)"'
