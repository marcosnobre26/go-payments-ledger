-- System accounts now have a role:
--   settlement: counterpart of money entering or leaving the platform
--   clearing:   funds reserved for payouts that are still in flight
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS system_role TEXT;
UPDATE accounts SET system_role = 'settlement' WHERE is_system AND system_role IS NULL;
DROP INDEX IF EXISTS uniq_system_account_per_currency;
CREATE UNIQUE INDEX IF NOT EXISTS uniq_system_account_per_role
    ON accounts (currency, system_role) WHERE is_system;

ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_kind_check;
ALTER TABLE transactions ADD CONSTRAINT transactions_kind_check
    CHECK (kind IN ('transfer', 'deposit', 'payout_reserve', 'payout_settle', 'payout_reverse'));

-- A payout sends money from an account to an external destination through a
-- payment provider. The row is committed (status 'pending', funds reserved)
-- BEFORE the provider is called, so the intent survives a crash.
CREATE TABLE IF NOT EXISTS payouts (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID        NOT NULL REFERENCES accounts (id),
    amount           BIGINT      NOT NULL CHECK (amount > 0),
    currency         CHAR(3)     NOT NULL,
    destination      TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'submitted', 'paid', 'failed')),
    idempotency_key  TEXT        NOT NULL UNIQUE,
    request_hash     TEXT        NOT NULL,
    provider_ref     TEXT,
    attempts         INT         NOT NULL DEFAULT 0,
    last_error       TEXT,
    -- When the reconciler should look at this payout again.
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payouts_in_flight
    ON payouts (next_attempt_at) WHERE status IN ('pending', 'submitted');
