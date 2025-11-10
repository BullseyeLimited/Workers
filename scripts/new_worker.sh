#!/usr/bin/env bash
set -e
NAME="$1"                               # e.g. zeus
APP="${NAME//_/-}-worker"
REGION="${2:-fra}"

if [[ -z "$NAME" ]]; then
  echo "usage: $0 <worker_name> [region]" >&2
  exit 1
fi

echo "→ scaffolding $NAME"
mkdir -p "workers/$NAME"

cat > "workers/$NAME/fly.toml" <<TOML
app = "$APP"
primary_region = "$REGION"

[build]
  dockerfile = "../../base/Dockerfile"

[build.args]
  ENTRYPOINT_WILL_BE_REPLACED = "workers/$NAME/main.py"

[[vm]]
  cpu_kind = "shared"
  cpus = 1
  memory = "1gb"
TOML

cat > "workers/$NAME/main.py" <<PY
print("$NAME placeholder — add real code")
PY
echo "✓ workers/$NAME ready"
