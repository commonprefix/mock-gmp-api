#!/bin/bash

# Script to check Axelar transaction status
# Usage: ./tx_status.sh <broadcast_id> <contract_address> <transaction_hash>

BROADCAST_ID=$1
CONTRACT_ADDRESS=$2
TX_HASH=$3

echo "=== Axelar Transaction Status Check ==="
echo "Timestamp: $(date)"
echo "Broadcast ID: $BROADCAST_ID"
echo "Contract Address: $CONTRACT_ADDRESS"
echo "Transaction Hash: $TX_HASH"

# Clean the transaction hash (remove 0x prefix if present)
CLEAN_TX_HASH=${TX_HASH#0x}
echo "Cleaned TX Hash: $CLEAN_TX_HASH"

if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    export $(grep -v '^#' .env | xargs)
else
    echo "Warning: .env file not found in current directory"
fi

# Check if axelard command is available
if ! command -v axelard &> /dev/null; then
    echo "ERROR: axelard command not found"
    # Should this be a failed status?
    jq -n '{"status": "FAILED", "error": "axelard command not available"}' > script_result.json
    exit 0
fi

if [ -z "$AXELAR_RPC" ]; then
    echo "ERROR: AXELAR_RPC environment variable not set"
    jq -n '{"status": "FAILED", "error": "AXELAR_RPC not configured"}' > script_result.json
    exit 0
fi

echo "Querying blockchain with axelard..."
echo "Using RPC: $AXELAR_RPC"

# Query the transaction
if axelard query tx "$CLEAN_TX_HASH" --node "$AXELAR_RPC" --output json > tx_status.json 2>&1; then
    echo "Query command executed successfully"
    
    # Check if the response file was created and is not empty
    if [ ! -f tx_status.json ] || [ ! -s tx_status.json ]; then
        echo "ERROR: Empty or missing response file"
        jq -n '{"status": "FAILED", "error": "Empty response from blockchain query"}' > script_result.json
        exit 0
    fi
    
    echo "Response received, parsing transaction status..."
    
    # Try to parse the transaction code
    TX_CODE=$(jq -r '.code // "unknown"' tx_status.json 2>/dev/null)
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to parse JSON response"
        jq -n '{"status": "FAILED", "error": "Invalid JSON response from blockchain"}' > script_result.json
    elif [ "$TX_CODE" = "0" ]; then
        echo "SUCCESS: Transaction executed successfully (code: 0)"
        jq -n '{"status": "SUCCESS"}' > script_result.json
    elif [ "$TX_CODE" = "unknown" ] || [ "$TX_CODE" = "null" ]; then
        echo "ERROR: Could not determine transaction status"
        jq -n '{"status": "FAILED", "error": "Unable to parse transaction status from response"}' > script_result.json
    else
        echo "FAILED: Transaction failed with code: $TX_CODE"
        TX_RAW_LOG=$(jq -r '.raw_log // "Transaction execution failed"' tx_status.json 2>/dev/null)
        echo "Error details: $TX_RAW_LOG"
        jq -n --arg error "$TX_RAW_LOG" --arg code "$TX_CODE" '{"status": "FAILED", "error": ("Transaction failed (code " + $code + "): " + $error)}' > script_result.json
    fi
    
else
    echo "Transaction not found - may still be pending"
    jq -n '{"status": "RECEIVED", "error": "Transaction not yet confirmed"}' > script_result.json
fi

[ -f tx_status.json ] && rm -f tx_status.json

echo "Script completed - result written to script_result.json"

if [ -f script_result.json ]; then
    echo "Result:"
    cat script_result.json
fi