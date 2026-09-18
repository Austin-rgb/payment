-- Idempotency log for settled debits; also doubles as the debit half of a
-- user's statement (see `PaymentRepository::statement`). `id` is the
-- caller-supplied `debit_id` — deliberately NOT server-generated, since its
-- whole purpose is to let a re-delivered broker message be recognised as
-- already-processed.
CREATE TABLE IF NOT EXISTS processed_debits (
 id UUID PRIMARY KEY,
 order_id UUID NOT NULL,
 user_id UUID NOT NULL REFERENCES accounts(id),
 amount BIGINT NOT NULL CHECK (amount >= 0),
 created TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_processed_debits_user_created ON processed_debits(user_id, created);
