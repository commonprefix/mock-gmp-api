#!/bin/bash

# Script to execute Axelar broadcast transactions
# Usage: ./execute_broadcast.sh <broadcast_id> <contract_address> <broadcast_payload_json>

BROADCAST_ID=$1
CONTRACT_ADDRESS=$2
BROADCAST_PAYLOAD=$3

if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Check if axelard command is available
if ! command -v axelard &> /dev/null; then
    echo '{"status": "FAILED", "error": "axelard command not available"}'
    exit 0
fi

# Check required environment variables
if [ -z "$AXELAR_RPC" ]; then
    echo '{"status": "FAILED", "error": "AXELAR_RPC not configured"}'
    exit 0
fi

if [ -z "$AXELAR_CHAIN_ID" ]; then
    echo '{"status": "FAILED", "error": "AXELAR_CHAIN_ID not configured"}'
    exit 0
fi

if [ -z "$AXELAR_WALLET" ]; then
    echo '{"status": "FAILED", "error": "AXELAR_WALLET not configured"}'
    exit 0
fi

# Parse the broadcast payload to extract the wasm execute data
WASM_MSG=$(echo "$BROADCAST_PAYLOAD" | jq -r '
  if has("VerifyMessages") then
    {verify_messages: .VerifyMessages}
  elif has("TicketCreate") then
    .TicketCreate
  elif has("RouteIncomingMessages") then
    {route_incoming_messages: .RouteIncomingMessages}
  else
    empty
  end
')

if [ -z "$WASM_MSG" ] || [ "$WASM_MSG" = "null" ]; then
    echo '{"status": "FAILED", "error": "Invalid broadcast payload format"}'
    exit 0
fi

# Execute the transaction
echo "Executing: axelard tx wasm execute $CONTRACT_ADDRESS '$WASM_MSG'" >&2

TX_OUTPUT=$(axelard tx wasm execute "$CONTRACT_ADDRESS" "$WASM_MSG" \
    --from "$AXELAR_WALLET" \
    --keyring-backend test \
    --node "$AXELAR_RPC" \
    --chain-id "$AXELAR_CHAIN_ID" \
    --gas-prices 0.00005uamplifier \
    --gas auto \
    --gas-adjustment 1.5 \
    --output json \
    --yes 2>&1)

TX_RESULT=$?

if [ $TX_RESULT -eq 0 ]; then
    # Parse transaction hash from output
    TX_HASH=$(echo "$TX_OUTPUT" | jq -r '.txhash // empty')
    
    if [ -n "$TX_HASH" ] && [ "$TX_HASH" != "null" ]; then
        echo "{\"status\": \"SUCCESS\", \"tx_hash\": \"$TX_HASH\"}"
    else
        echo "{\"status\": \"FAILED\", \"error\": \"Transaction submitted but no txhash returned\", \"output\": \"$TX_OUTPUT\"}"
    fi
else
    # Extract error message from output
    ERROR_MSG=$(echo "$TX_OUTPUT" | jq -r '.raw_log // .message // empty' 2>/dev/null || echo "$TX_OUTPUT")
    if [ -z "$ERROR_MSG" ]; then
        ERROR_MSG="Transaction execution failed"
    fi
    echo "{\"status\": \"FAILED\", \"error\": \"$ERROR_MSG\"}"
fi

# Clean up any temporary files
rm -f tx_output.json 