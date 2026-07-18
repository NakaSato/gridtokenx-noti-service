#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Default configuration
REST_PORT=${REST_PORT:-8080}
GRPC_PORT=${GRPC_PORT:-8090}
JWT_SECRET=${JWT_SECRET:-"dev-jwt-secret-key-minimum-32-characters-long-for-development-2025"}
USER_ID=${USER_ID:-"00000000-0000-0000-0000-000000000001"}

REST_URL="http://localhost:${REST_PORT}"
GRPC_URL="http://localhost:${GRPC_PORT}"

echo "=========================================================="
echo "⚡ Testing gridtokenx-noti-service Endpoints"
echo "REST URL: $REST_URL"
echo "ConnectRPC/gRPC URL: $GRPC_URL"
echo "Test User ID: $USER_ID"
echo "=========================================================="

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper for testing endpoints with body output
test_endpoint() {
    local method=$1
    local url=$2
    local expected=$3
    local headers=$4
    local data=$5
    
    echo -e "${BLUE}▶ $method $url${NC}"
    
    # Construct curl command
    local cmd="curl -s -w \"\\n---HTTP_STATUS:%{http_code}---\" -X $method"
    if [ -n "$headers" ]; then
        cmd="$cmd $headers"
    fi
    if [ -n "$data" ]; then
        cmd="$cmd -d '$data'"
    fi
    cmd="$cmd \"$url\""
    
    local response
    response=$(eval "$cmd")
    
    # Extract body and status code
    local body
    body=$(echo "$response" | sed '/---HTTP_STATUS:/d')
    local status
    status=$(echo "$response" | grep -o -e '---HTTP_STATUS:[0-9]*---' | grep -o -e '[0-9]*')
    
    echo "Body: $body"
    
    if [ "$status" -eq "$expected" ]; then
        echo -e "${GREEN}✔ PASS (Status: $status)${NC}"
    else
        echo -e "${RED}✘ FAIL (Expected: $expected, Got: $status)${NC}"
        exit 1
    fi
    echo ""
}

# 1. Health Endpoints
test_endpoint "GET" "$REST_URL/health" 200
test_endpoint "GET" "$REST_URL/health/live" 200

# 2. REST Notification APIs
echo "Checking REST Authorization requirements:"
# Should return 401 Unauthorized since x-gridtokenx-user-id header is missing
test_endpoint "GET" "$REST_URL/api/v1/noti" 401

# Authorized GET notifications list
test_endpoint "GET" "$REST_URL/api/v1/noti" 200 "-H 'x-gridtokenx-user-id: $USER_ID' -H 'x-gridtokenx-role: admin'"

# Authorized mark-all-read
test_endpoint "POST" "$REST_URL/api/v1/noti/read-all" 200 "-H 'x-gridtokenx-user-id: $USER_ID' -H 'x-gridtokenx-role: admin'"

# 3. ConnectRPC / gRPC Endpoints via standard JSON
echo "Testing ConnectRPC Endpoints (HTTP JSON):"

# Queue a notification
SEND_DATA='{
  "user_id": "'"$USER_ID"'",
  "channel": 0,
  "recipient": "test@gridtokenx.com",
  "template_id": "welcome.txt.tera",
  "variables_json": "{\\\"name\\\":\\\"Alice\\\"}",
  "idempotency_key": "test-key-'$(date +%s)'"
}'

echo -e "${BLUE}▶ POST $GRPC_URL/noti.v1.NotificationService/SendNotification${NC}"
echo "Request Body: $SEND_DATA"
RESPONSE=$(curl -s -X POST \
  -H "Content-Type: application/json" \
  -d "$SEND_DATA" \
  "$GRPC_URL/noti.v1.NotificationService/SendNotification")

echo "Response Body: $RESPONSE"

NOTI_ID=$(echo "$RESPONSE" | grep -o '"notificationId":"[^"]*' | grep -o '[^"]*$')

if [ -n "$NOTI_ID" ]; then
    echo -e "${GREEN}✔ SendNotification SUCCESS (ID: $NOTI_ID)${NC}\n"
    
    # Query its status
    STATUS_DATA='{
      "notificationId": "'"$NOTI_ID"'"
    }'
    test_endpoint "POST" "$GRPC_URL/noti.v1.NotificationService/GetNotificationStatus" 200 "-H 'Content-Type: application/json'" "$STATUS_DATA"
else
    echo -e "${RED}✘ SendNotification FAILED${NC}"
    exit 1
fi

# 4. WebSocket Auth validation
echo "Testing WebSocket Connection Auth:"

# Construct valid JWT using OpenSSL
JWT_HEADER_B64="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" # {"alg":"HS256","typ":"JWT"}
EXP=$(($(date +%s) + 3600))
JWT_PAYLOAD='{"sub":"'"$USER_ID"'","exp":'$EXP'}'
JWT_PAYLOAD_B64=$(echo -n "$JWT_PAYLOAD" | openssl base64 -e | tr -d '=' | tr '/+' '_-' | tr -d '\n' | tr -d '\r')
JWT_SIGNATURE_B64=$(echo -n "$JWT_HEADER_B64.$JWT_PAYLOAD_B64" | openssl dgst -sha256 -hmac "$JWT_SECRET" -binary | openssl base64 -e | tr -d '=' | tr '/+' '_-' | tr -d '\n' | tr -d '\r')
JWT_TOKEN="$JWT_HEADER_B64.$JWT_PAYLOAD_B64.$JWT_SIGNATURE_B64"

# Try connecting without token (should return 400 Bad Request)
test_endpoint "GET" "$REST_URL/ws" 400

# Perform the WebSocket handshake (using curl -H "Upgrade: websocket" with timeout to assert server accepts connection)
echo -e "${BLUE}▶ GET $REST_URL/ws?token=... (WebSocket Upgrade)${NC}"
WS_STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 2 \
  -H "Upgrade: websocket" \
  -H "Connection: Upgrade" \
  -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  -H "Sec-WebSocket-Version: 13" \
  "$REST_URL/ws?token=$JWT_TOKEN" || true)

if [ "$WS_STATUS" -eq 101 ]; then
    echo -e "${GREEN}✔ PASS (101 Switching Protocols)${NC}"
else
    echo -e "${RED}✘ FAIL (Expected 101, got $WS_STATUS)${NC}"
    exit 1
fi

echo -e "\n✅ All endpoint checks completed successfully."
