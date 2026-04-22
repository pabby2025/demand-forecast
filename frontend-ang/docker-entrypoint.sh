#!/bin/sh
# Inject runtime environment variables into nginx config via envsubst.
# BACKEND_URL defaults to http://backend:8000 (docker-compose local dev).
export BACKEND_URL="${BACKEND_URL:-http://backend:8000}"

envsubst '${BACKEND_URL}' \
  < /etc/nginx/templates/default.conf.template \
  > /etc/nginx/conf.d/default.conf

exec nginx -g "daemon off;"
