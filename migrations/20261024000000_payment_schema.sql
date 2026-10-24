-- Payment service schema. Idempotent: safe to apply on top of an existing database.
-- (The transactional outbox table `outbox_events` is created by ferrumec's `Outbox::new`.)

CREATE TABLE IF NOT EXISTS accounts (
    id      UUID   PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0)
);

CREATE TABLE IF NOT EXISTS credits (
    id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID        NOT NULL REFERENCES accounts(id),
    amount  BIGINT      NOT NULL CHECK (amount >= 0),
    created TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_credits_user_created ON credits(user_id, created);

-- Idempotency log for debits; doubles as the debit history for statements.
CREATE TABLE IF NOT EXISTS processed_debits (
    id       UUID        PRIMARY KEY,
    order_id UUID        NOT NULL,
    user_id  UUID        NOT NULL REFERENCES accounts(id),
    amount   BIGINT      NOT NULL CHECK (amount >= 0),
    created  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_processed_debits_user_created ON processed_debits(user_id, created);
