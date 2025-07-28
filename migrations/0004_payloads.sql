CREATE TABLE IF NOT EXISTS payloads (
    id	                TEXT NOT NULL PRIMARY KEY,
    payload	            TEXT NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);