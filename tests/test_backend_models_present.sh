#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODELS_DIR="$ROOT_DIR/backend/app/models"

for file in __init__.py base.py asset.py artifact.py job.py material.py pipeline.py schedule.py; do
  if [[ ! -f "$MODELS_DIR/$file" ]]; then
    printf 'FAIL: missing backend ORM model file: %s\n' "$MODELS_DIR/$file" >&2
    exit 1
  fi
done

grep -Fq 'from app.models import Base' "$ROOT_DIR/backend/alembic/env.py"
grep -Fq 'from app.models.artifact import Artifact, ArtifactKind' "$ROOT_DIR/backend/worker/main.py"
