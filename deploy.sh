#!/usr/bin/env bash
# =============================================================================
# Demand Forecast Planner — One-command deployment script
# Usage:  ./deploy.sh            (first time or full rebuild)
#         ./deploy.sh --no-build (restart existing containers)
#         ./deploy.sh --down     (stop and remove containers)
# =============================================================================
set -euo pipefail

COMPOSE="docker compose"
# Fall back to docker-compose v1 if docker compose plugin not found
if ! docker compose version &>/dev/null 2>&1; then
  COMPOSE="docker-compose"
fi

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$PROJECT_DIR/.env"
ENV_EXAMPLE="$PROJECT_DIR/.env.example"

# ── Flags ────────────────────────────────────────────────────────────────────
NO_BUILD=false
BRING_DOWN=false
for arg in "$@"; do
  case "$arg" in
    --no-build) NO_BUILD=true ;;
    --down)     BRING_DOWN=true ;;
  esac
done

# ── Stop ─────────────────────────────────────────────────────────────────────
if $BRING_DOWN; then
  echo "⏹  Stopping all containers..."
  $COMPOSE -f "$PROJECT_DIR/docker-compose.yml" down
  exit 0
fi

# ── Env file ─────────────────────────────────────────────────────────────────
if [ ! -f "$ENV_FILE" ]; then
  echo "📋 No .env found — copying from .env.example"
  cp "$ENV_EXAMPLE" "$ENV_FILE"
  echo "   Edit $ENV_FILE to set your passwords, then re-run this script."
fi

# ── Build & start ─────────────────────────────────────────────────────────────
cd "$PROJECT_DIR"

if $NO_BUILD; then
  echo "🚀 Starting containers (no rebuild)..."
  $COMPOSE up -d
else
  echo "🔨 Building images and starting containers..."
  $COMPOSE up --build -d
fi

# ── Wait for healthy state ────────────────────────────────────────────────────
echo ""
echo "⏳ Waiting for services to become healthy..."
ATTEMPTS=0
MAX=30
until $COMPOSE ps | grep -q "healthy" || [ $ATTEMPTS -ge $MAX ]; do
  sleep 3
  ATTEMPTS=$((ATTEMPTS + 1))
  printf "."
done
echo ""

# ── Status ────────────────────────────────────────────────────────────────────
echo ""
echo "📊 Container status:"
$COMPOSE ps

FRONTEND_PORT=$(grep FRONTEND_PORT "$ENV_FILE" | cut -d= -f2 | tr -d ' ' || echo 3000)
BACKEND_PORT=$(grep BACKEND_PORT "$ENV_FILE" | cut -d= -f2 | tr -d ' ' || echo 8000)

echo ""
echo "✅ Deployment complete!"
echo ""
echo "   🌐 App:     http://localhost:${FRONTEND_PORT:-3000}"
echo "   📡 API:     http://localhost:${BACKEND_PORT:-8000}/docs"
echo "   🗄️  DB:      localhost:${DB_PORT:-5432}  (demand_forecast_db)"
echo ""
echo "   Logs:   docker compose logs -f"
echo "   Stop:   ./deploy.sh --down"
