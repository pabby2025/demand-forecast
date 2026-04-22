#!/usr/bin/env bash
# =============================================================================
# Demand Forecast Planner — Azure teardown
# Deletes the entire resource group and all resources within it.
# Load values from azure/.env.azure if present, otherwise use defaults.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env.azure"

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

RESOURCE_GROUP="${RESOURCE_GROUP:-dfc-rg}"

if ! command -v az &>/dev/null; then
  echo "Azure CLI not found."
  exit 1
fi

echo ""
echo "⚠️  This will permanently delete resource group: $RESOURCE_GROUP"
echo "   All Container Apps, ACR images, and PostgreSQL data will be lost."
echo ""
read -r -p "Type the resource group name to confirm: " CONFIRM

if [ "$CONFIRM" != "$RESOURCE_GROUP" ]; then
  echo "Aborted — name did not match."
  exit 1
fi

echo ""
echo "▶  Deleting resource group $RESOURCE_GROUP..."
az group delete --name "$RESOURCE_GROUP" --yes --no-wait
echo "   ✅ Deletion initiated (runs in background, takes ~2 minutes)"
echo ""
echo "   Monitor: az group show --name $RESOURCE_GROUP --query properties.provisioningState -o tsv"
echo ""
