CREATE TYPE task_type as ENUM ('VERIFY', 'EXECUTE', 'GATEWAY_TX', 'CONSTRUCT_PROOF', 'REACT_TO_WASM_EVENT', 'REFUND', 'REACT_TO_EXPIRED_SIGNING_SESSION', 'REACT_TO_RETRIABLE_POLL', 'UNKNOWN');

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT NOT NULL PRIMARY KEY,
    chain TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    type task_type NOT NULL,
    meta TEXT DEFAULT NULL,
    task TEXT NOT NULL DEFAULT '{}'  -- should have no default probably, leaving it as is for simplicity
)