-- Append-only credit ledger. `id` is server-assigned (`gen_random_uuid()`),
-- mirroring `contributions`' migration style.
CREATE TABLE IF NOT EXISTS credits (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 user_id UUID NOT NULL REFERENCES accounts(id),
 amount BIGINT NOT NULL CHECK (amount >= 0),
 created TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_credits_user_created ON credits(user_id, created);
