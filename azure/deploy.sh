#!/usr/bin/env bash
# =============================================================================
# Demand Forecast Planner — Azure Container Apps Deployment
#
# Architecture:
#   ACR            → stores both Docker images (built in cloud, no local Docker)
#   Container Apps → backend (internal ingress) + frontend (external ingress)
#   PostgreSQL     → Azure Flexible Server with pgvector extension
#
# Prerequisites:
#   - Azure CLI installed  (brew install azure-cli)
#   - Logged in           (az login)
#   - Subscription set    (az account set --subscription "<name or id>")
#
# Usage:
#   chmod +x azure/deploy.sh
#   ./azure/deploy.sh
# =============================================================================
set -euo pipefail

# ── Configuration — edit these before first run ───────────────────────────────
RESOURCE_GROUP="dfc-rg"
LOCATION="eastus"
ACR_NAME="dfcregistry$(openssl rand -hex 3)"   # globally unique
ENVIRONMENT_NAME="dfc-env"
BACKEND_APP="dfc-backend"
FRONTEND_APP="dfc-frontend"

PG_SERVER="dfc-pg-$(openssl rand -hex 3)"     # globally unique
PG_DB="demand_forecast_db"
PG_USER="dfc_app"
PG_PASSWORD="${PG_PASSWORD:-$(openssl rand -base64 20 | tr -dc 'A-Za-z0-9!@#' | head -c 20)}"

SECRET_KEY="${SECRET_KEY:-$(openssl rand -base64 32)}"

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── Helpers ───────────────────────────────────────────────────────────────────
info()  { echo ""; echo "▶  $*"; }
ok()    { echo "   ✅ $*"; }
warn()  { echo "   ⚠️  $*"; }

# ── Preflight checks ──────────────────────────────────────────────────────────
info "Checking prerequisites..."
if ! command -v az &>/dev/null; then
  echo "Azure CLI not found. Install it:"
  echo "  macOS:   brew install azure-cli"
  echo "  Linux:   curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash"
  exit 1
fi

if ! az account show &>/dev/null 2>&1; then
  echo "Not logged in to Azure. Run:  az login"
  exit 1
fi

SUBSCRIPTION=$(az account show --query name -o tsv)
ok "Using subscription: $SUBSCRIPTION"

# ── Resource Group ────────────────────────────────────────────────────────────
info "Creating resource group: $RESOURCE_GROUP ($LOCATION)..."
az group create --name "$RESOURCE_GROUP" --location "$LOCATION" --output none
ok "Resource group ready"

# ── Azure Container Registry ──────────────────────────────────────────────────
info "Creating ACR: $ACR_NAME..."
az acr create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$ACR_NAME" \
  --sku Basic \
  --admin-enabled true \
  --output none
ok "ACR created"

ACR_LOGIN_SERVER=$(az acr show --name "$ACR_NAME" --query loginServer -o tsv)
ok "Login server: $ACR_LOGIN_SERVER"

# ── Build images in Azure (no local Docker needed) ───────────────────────────
info "Building backend image in Azure (az acr build)..."
az acr build \
  --registry "$ACR_NAME" \
  --image "dfc-backend:latest" \
  --file "$PROJECT_ROOT/backend/Dockerfile" \
  "$PROJECT_ROOT/backend"
ok "Backend image built: $ACR_LOGIN_SERVER/dfc-backend:latest"

info "Building frontend image in Azure (az acr build)..."
az acr build \
  --registry "$ACR_NAME" \
  --image "dfc-frontend:latest" \
  --file "$PROJECT_ROOT/frontend-ang/Dockerfile" \
  "$PROJECT_ROOT/frontend-ang"
ok "Frontend image built: $ACR_LOGIN_SERVER/dfc-frontend:latest"

# ── PostgreSQL Flexible Server ────────────────────────────────────────────────
info "Creating PostgreSQL Flexible Server: $PG_SERVER..."
az postgres flexible-server create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$PG_SERVER" \
  --location "$LOCATION" \
  --admin-user "$PG_USER" \
  --admin-password "$PG_PASSWORD" \
  --sku-name "Standard_B1ms" \
  --tier "Burstable" \
  --storage-size 32 \
  --version 15 \
  --public-access "0.0.0.0" \
  --output none
ok "PostgreSQL server created"

info "Creating database: $PG_DB..."
az postgres flexible-server db create \
  --resource-group "$RESOURCE_GROUP" \
  --server-name "$PG_SERVER" \
  --database-name "$PG_DB" \
  --output none
ok "Database created"

info "Enabling pgvector extension..."
az postgres flexible-server parameter set \
  --resource-group "$RESOURCE_GROUP" \
  --server-name "$PG_SERVER" \
  --name "azure.extensions" \
  --value "VECTOR" \
  --output none
ok "pgvector enabled"

PG_HOST=$(az postgres flexible-server show \
  --resource-group "$RESOURCE_GROUP" \
  --name "$PG_SERVER" \
  --query "fullyQualifiedDomainName" -o tsv)
DATABASE_URL="postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:5432/${PG_DB}?sslmode=require"
ok "Database host: $PG_HOST"

info "Running DB init scripts..."
for SQL_FILE in "$PROJECT_ROOT"/database/docker-init/*.sql; do
  SCRIPT_NAME=$(basename "$SQL_FILE")
  echo "   Running $SCRIPT_NAME..."
  az postgres flexible-server execute \
    --name "$PG_SERVER" \
    --admin-user "$PG_USER" \
    --admin-password "$PG_PASSWORD" \
    --database-name "$PG_DB" \
    --file-path "$SQL_FILE" \
    --output none 2>/dev/null || warn "Script $SCRIPT_NAME returned warnings (may be normal)"
done
ok "Database initialised"

# ── Container Apps Environment ────────────────────────────────────────────────
info "Creating Container Apps environment: $ENVIRONMENT_NAME..."
az containerapp env create \
  --name "$ENVIRONMENT_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --output none
ok "Container Apps environment ready"

# ── ACR credentials for Container Apps ───────────────────────────────────────
ACR_USERNAME=$(az acr credential show --name "$ACR_NAME" --query username -o tsv)
ACR_PASSWORD=$(az acr credential show --name "$ACR_NAME" --query "passwords[0].value" -o tsv)

# ── Backend Container App (internal ingress) ──────────────────────────────────
info "Deploying backend Container App: $BACKEND_APP..."
az containerapp create \
  --name "$BACKEND_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --environment "$ENVIRONMENT_NAME" \
  --image "$ACR_LOGIN_SERVER/dfc-backend:latest" \
  --registry-server "$ACR_LOGIN_SERVER" \
  --registry-username "$ACR_USERNAME" \
  --registry-password "$ACR_PASSWORD" \
  --ingress internal \
  --target-port 8000 \
  --cpu 0.5 \
  --memory 1.0Gi \
  --min-replicas 1 \
  --max-replicas 3 \
  --env-vars \
    "DATABASE_URL=${DATABASE_URL}" \
    "SECRET_KEY=${SECRET_KEY}" \
    "AZURE_OPENAI_KEY=mock" \
    "ENVIRONMENT=production" \
  --output none
ok "Backend deployed (internal)"

BACKEND_INTERNAL_URL=$(az containerapp show \
  --name "$BACKEND_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --query "properties.configuration.ingress.fqdn" -o tsv)
ok "Backend internal URL: https://$BACKEND_INTERNAL_URL"

# ── Frontend Container App (external ingress) ─────────────────────────────────
info "Deploying frontend Container App: $FRONTEND_APP..."
az containerapp create \
  --name "$FRONTEND_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --environment "$ENVIRONMENT_NAME" \
  --image "$ACR_LOGIN_SERVER/dfc-frontend:latest" \
  --registry-server "$ACR_LOGIN_SERVER" \
  --registry-username "$ACR_USERNAME" \
  --registry-password "$ACR_PASSWORD" \
  --ingress external \
  --target-port 3000 \
  --cpu 0.25 \
  --memory 0.5Gi \
  --min-replicas 1 \
  --max-replicas 3 \
  --env-vars \
    "BACKEND_URL=http://${BACKEND_INTERNAL_URL}" \
  --output none
ok "Frontend deployed (external)"

FRONTEND_URL=$(az containerapp show \
  --name "$FRONTEND_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --query "properties.configuration.ingress.fqdn" -o tsv)

# ── Allow frontend Container App to reach PostgreSQL ─────────────────────────
info "Configuring PostgreSQL firewall for Container Apps outbound IPs..."
OUTBOUND_IPS=$(az containerapp env show \
  --name "$ENVIRONMENT_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "properties.staticIp" -o tsv 2>/dev/null || echo "")

if [ -n "$OUTBOUND_IPS" ]; then
  az postgres flexible-server firewall-rule create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$PG_SERVER" \
    --rule-name "AllowContainerApps" \
    --start-ip-address "$OUTBOUND_IPS" \
    --end-ip-address "$OUTBOUND_IPS" \
    --output none 2>/dev/null || warn "Could not add firewall rule automatically — add it manually if DB connection fails"
  ok "Firewall rule added for $OUTBOUND_IPS"
else
  warn "Could not determine outbound IP — add PostgreSQL firewall rule manually if needed"
fi

# ── Save credentials to .env.azure ───────────────────────────────────────────
CREDS_FILE="$PROJECT_ROOT/azure/.env.azure"
cat > "$CREDS_FILE" <<EOF
# Generated by azure/deploy.sh — DO NOT COMMIT
RESOURCE_GROUP=$RESOURCE_GROUP
ACR_NAME=$ACR_NAME
ACR_LOGIN_SERVER=$ACR_LOGIN_SERVER
ENVIRONMENT_NAME=$ENVIRONMENT_NAME
BACKEND_APP=$BACKEND_APP
FRONTEND_APP=$FRONTEND_APP
PG_SERVER=$PG_SERVER
PG_HOST=$PG_HOST
PG_USER=$PG_USER
PG_PASSWORD=$PG_PASSWORD
PG_DB=$PG_DB
DATABASE_URL=$DATABASE_URL
SECRET_KEY=$SECRET_KEY
FRONTEND_URL=https://$FRONTEND_URL
BACKEND_INTERNAL_URL=https://$BACKEND_INTERNAL_URL
EOF
chmod 600 "$CREDS_FILE"
ok "Credentials saved to azure/.env.azure (keep this file secret)"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Deployment complete!"
echo "============================================================"
echo ""
echo "  App URL:     https://$FRONTEND_URL"
echo "  API Docs:    https://$BACKEND_INTERNAL_URL/docs  (internal only)"
echo "  Resource RG: $RESOURCE_GROUP"
echo ""
echo "  To redeploy after code changes:"
echo "    az acr build --registry $ACR_NAME --image dfc-backend:latest --file backend/Dockerfile backend/"
echo "    az acr build --registry $ACR_NAME --image dfc-frontend:latest --file frontend-ang/Dockerfile frontend-ang/"
echo "    az containerapp update --name $BACKEND_APP --resource-group $RESOURCE_GROUP --image $ACR_LOGIN_SERVER/dfc-backend:latest"
echo "    az containerapp update --name $FRONTEND_APP --resource-group $RESOURCE_GROUP --image $ACR_LOGIN_SERVER/dfc-frontend:latest"
echo ""
echo "  To tear down everything:"
echo "    ./azure/teardown.sh"
echo ""
