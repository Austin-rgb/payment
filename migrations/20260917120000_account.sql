-- Balances table. `id` is the user's id directly (one row per user) rather
-- than a synthetic surrogate key, matching the pre-existing `accounts`
-- table's single-column primary key. Kept as `id` (rather than `user_id`)
-- so the row maps directly onto `viewset::Entity`'s expected primary-key
-- field for the `/admin/accounts` viewset (see `src/admin.rs`).
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS accounts (
 id UUID PRIMARY KEY,
 balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0)
);
