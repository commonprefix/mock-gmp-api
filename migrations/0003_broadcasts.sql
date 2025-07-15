CREATE TYPE broadcast_status as ENUM ('RECEIVED', 'SUCCESS', 'FAILED');

CREATE TABLE IF NOT EXISTS broadcasts (
    id TEXT NOT NULL PRIMARY KEY,
    contract_address TEXT NOT NULL,
    broadcast TEXT NOT NULL,
    status broadcast_status NOT NULL DEFAULT 'RECEIVED',
    tx_hash TEXT,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);