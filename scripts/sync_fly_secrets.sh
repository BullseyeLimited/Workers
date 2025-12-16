#!/usr/bin/env bash
#
# Sync shared secrets to Fly apps in one command.
# 1) Create/edit .env.private at repo root with:
#    SUPABASE_URL=...
#    SUPABASE_SERVICE_ROLE_KEY=...
#    RUNPOD_URL=...               # optional
#    RUNPOD_API_KEY=...           # optional
#    OPENAI_API_KEY=...           # optional
#    OPENAI_BASE_URL=...          # optional
#    FLY_API_TOKEN=...            # optional (or login via flyctl auth login)
# 2) Run: ./scripts/sync_fly_secrets.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT}/.env.private"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing ${ENV_FILE}. Add your secrets there first." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

require() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required var ${name} in ${ENV_FILE}" >&2
    exit 1
  fi
}

require SUPABASE_URL
require SUPABASE_SERVICE_ROLE_KEY

if [[ -n "${FLY_API_TOKEN:-}" ]]; then
  export FLY_API_TOKEN
fi

set_hermes() {
  local args=(
    "SUPABASE_URL=${SUPABASE_URL}"
    "SUPABASE_SERVICE_ROLE_KEY=${SUPABASE_SERVICE_ROLE_KEY}"
  )
  [[ -n "${RUNPOD_URL:-}" ]] && args+=("RUNPOD_URL=${RUNPOD_URL}")
  [[ -n "${RUNPOD_API_KEY:-}" ]] && args+=("RUNPOD_API_KEY=${RUNPOD_API_KEY}")
  [[ -n "${OPENAI_API_KEY:-}" ]] && args+=("OPENAI_API_KEY=${OPENAI_API_KEY}")
  [[ -n "${OPENAI_BASE_URL:-}" ]] && args+=("OPENAI_BASE_URL=${OPENAI_BASE_URL}")
  flyctl secrets set "${args[@]}" -a hermes-worker
}

set_hermes_join() {
  flyctl secrets set \
    SUPABASE_URL="${SUPABASE_URL}" \
    SUPABASE_SERVICE_ROLE_KEY="${SUPABASE_SERVICE_ROLE_KEY}" \
    -a hermesjoin-worker
}

set_webresearch() {
  local args=(
    "SUPABASE_URL=${SUPABASE_URL}"
    "SUPABASE_SERVICE_ROLE_KEY=${SUPABASE_SERVICE_ROLE_KEY}"
  )
  [[ -n "${OPENAI_API_KEY:-}" ]] && args+=("OPENAI_API_KEY=${OPENAI_API_KEY}")
  [[ -n "${OPENAI_BASE_URL:-}" ]] && args+=("OPENAI_BASE_URL=${OPENAI_BASE_URL}")
  flyctl secrets set "${args[@]}" -a websearch-worker
}

echo "Syncing secrets to hermes-worker..."
set_hermes
echo "Syncing secrets to hermesjoin-worker..."
set_hermes_join
echo "Syncing secrets to websearch-worker..."
set_webresearch

echo "Done."
