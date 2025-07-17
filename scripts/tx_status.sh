#!/bin/bash

# Script to check Axelar transaction status
# Usage: ./tx_status.sh <broadcast_id> <contract_address> <transaction_hash>

BROADCAST_ID=$1
CONTRACT_ADDRESS=$2

if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Check if axelard command is available
if ! command -v axelard &> /dev/null; then
    # Should this be a failed status?
    echo '{"status": "FAILED", "error": "axelard command not available"}'
    exit 0
fi

if [ -z "$AXELAR_RPC" ]; then
    echo '{"status": "FAILED", "error": "AXELAR_RPC not configured"}'
    exit 0
fi

# Query the transaction
if axelard query tx "$BROADCAST_ID" --node "$AXELAR_RPC" --output json > tx_status.json 2>&1; then
    # Check if the response file was created and is not empty
    if [ ! -f tx_status.json ] || [ ! -s tx_status.json ]; then
        echo '{"status": "FAILED", "error": "Empty response from blockchain query"}'
        exit 0
    fi
    
    # Try to parse the transaction code
    TX_CODE=$(jq -r '.code // "unknown"' tx_status.json 2>/dev/null)
    
    if [ $? -ne 0 ]; then
        echo '{"status": "FAILED", "error": "Invalid JSON response from blockchain"}'
    elif [ "$TX_CODE" = "0" ]; then
        echo '{"status": "SUCCESS", "tx_hash": "'$BROADCAST_ID'"}'
    elif [ "$TX_CODE" = "unknown" ] || [ "$TX_CODE" = "null" ]; then
        echo '{"status": "FAILED", "error": "Unable to parse transaction status from response"}'
    else
        TX_RAW_LOG=$(jq -r '.raw_log // "Transaction execution failed"' tx_status.json 2>/dev/null)
        echo '{"status": "FAILED", "error": ("Transaction failed (code " + $TX_CODE + "): " + $TX_RAW_LOG)}'
    fi
    
else
    echo '{"status": "RECEIVED", "error": "Transaction not yet confirmed"}'
fi

# Clean up temporary files
rm -f tx_status.json
