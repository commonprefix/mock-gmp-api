CREATE TYPE event_type as ENUM ('CALL', 'GAS_REFUNDED', 'GAS_CREDIT', 'MESSAGE_EXECUTED', 'CANNOT_EXECUTE_MESSAGE_V2', 'ITS_INTERCHAIN_TRANSFER');

CREATE TABLE IF NOT EXISTS events (
    id TEXT NOT NULL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL, -- todo : down migration, make it timestamptz
    type event_type NOT NULL,
    event TEXT NOT NULL
);
