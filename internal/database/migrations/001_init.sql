-- Accounts hold a cached balance in minor units (cents). The source of truth
-- is ledger_entries; balance is updated in the same transaction as the entries.
CREATE TABLE IF NOT EXISTS accounts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    currency    CHAR(3)     NOT NULL,
    balance     BIGINT      NOT NULL DEFAULT 0,
    is_system   BOOLEAN     NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Customer accounts can never go negative; system (settlement) accounts can.
    CONSTRAINT balance_non_negative CHECK (is_system OR balance >= 0)
);

-- One settlement account per currency, used as the counterpart of deposits.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_system_account_per_currency
    ON accounts (currency) WHERE is_system;

CREATE TABLE IF NOT EXISTS transactions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind        TEXT        NOT NULL CHECK (kind IN ('transfer', 'deposit')),
    -- External reference (e.g. webhook event id). UNIQUE = second idempotency layer.
    reference   TEXT UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Double-entry ledger: every transaction has entries that sum to zero.
CREATE TABLE IF NOT EXISTS ledger_entries (
    id              BIGSERIAL PRIMARY KEY,
    transaction_id  UUID        NOT NULL REFERENCES transactions (id),
    account_id      UUID        NOT NULL REFERENCES accounts (id),
    amount          BIGINT      NOT NULL CHECK (amount <> 0),
    currency        CHAR(3)     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_account
    ON ledger_entries (account_id, id DESC);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key             TEXT PRIMARY KEY,
    request_hash    TEXT        NOT NULL,
    transaction_id  UUID REFERENCES transactions (id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS webhook_events (
    id               BIGSERIAL PRIMARY KEY,
    provider         TEXT        NOT NULL,
    event_id         TEXT        NOT NULL,
    event_type       TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'processed', 'failed')),
    attempts         INT         NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error       TEXT,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at     TIMESTAMPTZ,
    -- Providers retry deliveries: the same event must only be stored once.
    UNIQUE (provider, event_id)
);

CREATE INDEX IF NOT EXISTS idx_webhook_events_pending
    ON webhook_events (next_attempt_at) WHERE status = 'pending';
