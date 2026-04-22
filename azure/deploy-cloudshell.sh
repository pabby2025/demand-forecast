#!/usr/bin/env bash
# =============================================================================
# Demand Forecast Planner — Azure Cloud Shell Deployment (NO Docker / NO ACR)
#
# Architecture:
#   Backend  → Azure App Service (Python 3.11)   — zip deploy, no Docker
#   Frontend → Azure Blob Storage static website  — Angular SPA
#   Database → Azure PostgreSQL Flexible Server   — pgvector extension
#
# How to use:
#   1. Upload your project zip to Cloud Shell (Upload button in browser)
#      Create zip WITHOUT node_modules:
#        macOS/Linux: zip -r demand-forecast.zip . -x "*/node_modules/*" -x "*/__pycache__/*" -x "*/dist/*" -x "*/.git/*"
#        Windows:     Right-click → Send to Compressed folder (exclude node_modules manually)
#
#   2. In Cloud Shell, unzip and run this script:
#        unzip demand-forecast.zip -d demand-forecast && cd demand-forecast
#        chmod +x azure/deploy-cloudshell.sh
#        ./azure/deploy-cloudshell.sh
# =============================================================================
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
RESOURCE_GROUP="demand-forecast"
LOCATION="eastus"

# App Service names must be globally unique
SUFFIX="$(openssl rand -hex 3)"
BACKEND_APP="dfc-backend-${SUFFIX}"
ASP_NAME="dfc-asp-${SUFFIX}"

# Storage account name: 3-24 chars, lowercase letters and numbers only
STORAGE_ACCOUNT="dfcfront${SUFFIX}"

# PostgreSQL
PG_SERVER="dfc-pg-${SUFFIX}"
PG_DB="demand_forecast_db"
PG_USER="dfcapp"
PG_PASSWORD="DfcPwd$(openssl rand -hex 6)!"

SECRET_KEY="$(openssl rand -base64 32)"

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── Helpers ───────────────────────────────────────────────────────────────────
info() { echo ""; echo "▶  $*"; }
ok()   { echo "   ✅ $*"; }
warn() { echo "   ⚠️  $*"; }

echo ""
echo "======================================================"
echo "  Demand Forecast Planner — Azure Deployment"
echo "  Resource Group : $RESOURCE_GROUP"
echo "  Location       : $LOCATION"
echo "  Backend App    : $BACKEND_APP"
echo "  Storage Account: $STORAGE_ACCOUNT"
echo "======================================================"

# ── 1. PostgreSQL Flexible Server ─────────────────────────────────────────────
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
  --yes \
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
ok "Host: $PG_HOST"

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
    --output none 2>/dev/null || warn "$SCRIPT_NAME returned warnings (may be normal for re-runs)"
done
ok "Database initialised"

# ── 2. Backend — App Service (Python 3.11) ────────────────────────────────────
info "Creating App Service plan: $ASP_NAME (B1, Linux)..."
az appservice plan create \
  --name "$ASP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --sku B1 \
  --is-linux \
  --output none
ok "Plan created"

info "Creating Web App: $BACKEND_APP (Python 3.11)..."
az webapp create \
  --name "$BACKEND_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --plan "$ASP_NAME" \
  --runtime "PYTHON:3.11" \
  --output none
ok "Web App created"

info "Configuring backend app settings..."
az webapp config appsettings set \
  --name "$BACKEND_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --settings \
    "DATABASE_URL=${DATABASE_URL}" \
    "SECRET_KEY=${SECRET_KEY}" \
    "AZURE_OPENAI_KEY=mock" \
    "ENVIRONMENT=production" \
    "WEBSITES_PORT=8000" \
    "SCM_DO_BUILD_DURING_DEPLOYMENT=true" \
  --output none

# Set startup command — run uvicorn on port 8000
az webapp config set \
  --name "$BACKEND_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --startup-file "uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2" \
  --output none
ok "App settings configured"

info "Packaging and deploying backend..."
cd "$PROJECT_ROOT/backend"
zip -r /tmp/backend.zip . -x "__pycache__/*" "*.pyc" "*.pyo" 2>/dev/null
az webapp deploy \
  --name "$BACKEND_APP" \
  --resource-group "$RESOURCE_GROUP" \
  --src-path /tmp/backend.zip \
  --type zip \
  --output none
ok "Backend deployed"

BACKEND_URL="https://${BACKEND_APP}.azurewebsites.net"
ok "Backend URL: $BACKEND_URL"

# ── 3. Frontend — Build Angular + Blob Storage static website ─────────────────
info "Installing Node dependencies (this takes ~3-4 minutes)..."
cd "$PROJECT_ROOT/frontend-ang"
npm install --legacy-peer-deps --silent
ok "Dependencies installed"

info "Building Angular app for production..."
npm run build:prod
ok "Build complete"

# Inject the backend API URL into the built index.html so the app knows where to call
info "Injecting API URL into index.html..."
INDEX_HTML="$PROJECT_ROOT/frontend-ang/dist/demand-planning-frontend-ang/browser/index.html"
sed -i "s|</head>|<script>window.env={API_URL:\"${BACKEND_URL}\"};</script></head>|" "$INDEX_HTML"
ok "API URL injected: $BACKEND_URL"

# Create storage account for static website hosting
info "Creating storage account: $STORAGE_ACCOUNT..."
az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --allow-blob-public-access true \
  --output none
ok "Storage account created"

info "Enabling static website hosting..."
az storage blob service-properties update \
  --account-name "$STORAGE_ACCOUNT" \
  --static-website \
  --index-document "index.html" \
  --404-document "index.html" \
  --output none
ok "Static website enabled (Angular SPA routing supported via 404→index.html)"

info "Uploading Angular build to \$web container..."
az storage blob upload-batch \
  --account-name "$STORAGE_ACCOUNT" \
  --source "$PROJECT_ROOT/frontend-ang/dist/demand-planning-frontend-ang/browser" \
  --destination '$web' \
  --overwrite \
  --output none
ok "Frontend files uploaded"

FRONTEND_URL=$(az storage account show \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query "primaryEndpoints.web" -o tsv)

# ── 4. Allow App Service to reach PostgreSQL ──────────────────────────────────
info "Configuring PostgreSQL firewall to allow Azure services..."
az postgres flexible-server firewall-rule create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$PG_SERVER" \
  --rule-name "AllowAzureServices" \
  --start-ip-address "0.0.0.0" \
  --end-ip-address "0.0.0.0" \
  --output none
ok "Firewall rule added"

# ── 5. Save credentials ───────────────────────────────────────────────────────
CREDS_FILE="$PROJECT_ROOT/azure/.env.azure"
cat > "$CREDS_FILE" <<EOF
# Generated by azure/deploy-cloudshell.sh — DO NOT COMMIT
RESOURCE_GROUP=$RESOURCE_GROUP
BACKEND_APP=$BACKEND_APP
ASP_NAME=$ASP_NAME
STORAGE_ACCOUNT=$STORAGE_ACCOUNT
PG_SERVER=$PG_SERVER
PG_HOST=$PG_HOST
PG_USER=$PG_USER
PG_PASSWORD=$PG_PASSWORD
PG_DB=$PG_DB
DATABASE_URL=$DATABASE_URL
SECRET_KEY=$SECRET_KEY
BACKEND_URL=$BACKEND_URL
FRONTEND_URL=$FRONTEND_URL
EOF
chmod 600 "$CREDS_FILE"
ok "Credentials saved to azure/.env.azure"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Deployment complete!"
echo "============================================================"
echo ""
echo "  App URL:    $FRONTEND_URL"
echo "  API Docs:   $BACKEND_URL/docs"
echo "  API Health: $BACKEND_URL/health"
echo ""
echo "  To redeploy backend after changes:"
echo "    cd backend && zip -r /tmp/backend.zip . -x '__pycache__/*' '*.pyc'"
echo "    az webapp deploy --name $BACKEND_APP --resource-group $RESOURCE_GROUP --src-path /tmp/backend.zip --type zip"
echo ""
echo "  To redeploy frontend after changes:"
echo "    cd frontend-ang && npm run build:prod"
echo "    sed -i 's|</head>|<script>window.env={API_URL:\"$BACKEND_URL\"};</script></head>|' dist/demand-planning-frontend-ang/browser/index.html"
echo "    az storage blob upload-batch --account-name $STORAGE_ACCOUNT --source dist/demand-planning-frontend-ang/browser --destination '\$web' --overwrite"
echo ""
echo "  To tear down:"
echo "    ./azure/teardown.sh"
echo ""
