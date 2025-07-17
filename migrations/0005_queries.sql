CREATE TABLE IF NOT EXISTS queries (
    id	                TEXT NOT NULL PRIMARY KEY,
    contract_address    TEXT NOT NULL,
    query	            TEXT NOT NULL,
    result              TEXT,
    error               TEXT
);