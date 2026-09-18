//! Durable storage abstraction and its Postgres implementation.
//!
//! The repository handles the pieces of state that **must** survive a
//! process restart:
//! - account balances,
//! - the credit history (for statements),
//! - the processed-debit idempotency log, which also doubles as the debit
//!   history (for statements) since it already records amount, time, and
//!   order_id at the moment a debit settles.
//!
//! Events that need to reach a broker are handled separately (see
//! [`crate::subscriptions`] / [`typed_eventbus`]).
//!
//! # Schema
//!
//! This module assumes the DDL under `migrations/` has been applied
//! (`sqlx migrate run`):
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS accounts (
//!     id      UUID PRIMARY KEY,
//!     balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0)
//! );
//!
//! CREATE TABLE IF NOT EXISTS credits (
//!     id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//!     user_id    UUID NOT NULL REFERENCES accounts(id),
//!     amount     BIGINT NOT NULL CHECK (amount >= 0),
//!     created    TIMESTAMPTZ NOT NULL DEFAULT now()
//! );
//! CREATE INDEX IF NOT EXISTS idx_credits_user_created ON credits(user_id, created);
//!
//! CREATE TABLE IF NOT EXISTS processed_debits (
//!     id         UUID PRIMARY KEY,
//!     order_id   UUID NOT NULL,
//!     user_id    UUID NOT NULL REFERENCES accounts(id),
//!     amount     BIGINT NOT NULL CHECK (amount >= 0),
//!     created    TIMESTAMPTZ NOT NULL DEFAULT now()
//! );
//! CREATE INDEX IF NOT EXISTS idx_processed_debits_user_created ON processed_debits(user_id, created);
//! ```
//!
//! Unlike SQLite, Postgres has native `UUID` and `TIMESTAMPTZ` column types,
//! so — compared to the previous SQLite-backed implementation — this module
//! no longer round-trips ids/timestamps through strings; `sqlx` maps
//! [`Uuid`] and [`DateTime<Utc>`] directly.
//!
//! As with the previous SQLite implementation, queries here use
//! `sqlx::query` / `sqlx::query_as` (dynamic queries) rather than the
//! `query!`/`query_as!` macros, so building this crate does not require a
//! live database connection (or a `.sqlx` offline cache) — only the
//! `viewset`-backed admin CRUD in [`crate::admin`] uses `sqlx`'s
//! `FromRow`-derived, reflection-style mapping.

use crate::errors::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::models::LedgerEntry;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Persistent storage operations for the payment service.
#[async_trait]
pub trait PaymentRepository: Send + Sync + 'static {
    /// Add `amount` to `user_id`'s balance, creating the account row if it
    /// does not yet exist, and record a `credits` ledger row for `amount` at
    /// the current time.
    async fn credit(&self, user_id: Uuid, amount: u64) -> Result<()>;

    /// Attempt to atomically deduct `amount` from `user_id`'s balance.
    ///
    /// Returns `true` when the debit succeeded, `false` when the balance was
    /// insufficient. This method is **not** idempotent on its own; callers
    /// must check [`PaymentRepository::debit_processed`] first.
    async fn try_debit(&self, user_id: Uuid, amount: u64) -> Result<bool>;

    /// Return the current balance for `user_id`, or `0` if no account exists.
    async fn balance(&self, user_id: Uuid) -> Result<u64>;

    /// Return `true` if `debit_id` has already been fully processed.
    async fn debit_processed(&self, debit_id: Uuid) -> Result<bool>;

    /// Record `debit_id` as processed (idempotent — safe to call twice),
    /// storing the details needed to reconstruct it in a statement.
    async fn mark_processed(
        &self,
        debit_id: Uuid,
        order_id: Uuid,
        user_id: Uuid,
        amount: u64,
    ) -> Result<()>;

    /// Return all balance-affecting entries (credits and settled debits) for
    /// `user_id` at or after `since`, sorted newest first.
    async fn statement(&self, user_id: Uuid, since: DateTime<Utc>) -> Result<Vec<LedgerEntry>>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// [`PaymentRepository`] backed by a Postgres database via `sqlx`.
///
/// Replaces the previous `SqliteRepository`. All queries use `sqlx::query` /
/// `sqlx::query_as` (dynamic queries, `$n`-style placeholders) rather than
/// the `query!` macro so that no compile-time database connection is
/// required.
pub struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PaymentRepository for PostgresRepository {
    async fn credit(&self, user_id: Uuid, amount: u64) -> Result<()> {
        let amt = amount as i64;

        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "INSERT INTO accounts (id, balance) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET balance = accounts.balance + EXCLUDED.balance",
        )
        .bind(user_id)
        .bind(amt)
        .execute(&mut *tx)
        .await?;

        sqlx::query("INSERT INTO credits (user_id, amount) VALUES ($1, $2)")
            .bind(user_id)
            .bind(amt)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn try_debit(&self, user_id: Uuid, amount: u64) -> Result<bool> {
        let amt = amount as i64;

        let mut tx = self.pool.begin().await?;

        // `FOR UPDATE` locks the row for the remainder of the transaction so
        // two concurrent debits against the same account can't both read the
        // same balance and both succeed — a guarantee SQLite's
        // whole-database write lock gave us "for free" but that Postgres
        // needs an explicit row lock for under `READ COMMITTED`.
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT balance FROM accounts WHERE id = $1 FOR UPDATE")
                .bind(user_id)
                .fetch_optional(&mut *tx)
                .await?;

        let balance = match row {
            Some((b,)) => b,
            None => {
                tx.rollback().await?;
                return Ok(false);
            }
        };

        if balance < amt {
            tx.rollback().await?;
            return Ok(false);
        }

        sqlx::query("UPDATE accounts SET balance = balance - $1 WHERE id = $2")
            .bind(amt)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(true)
    }

    async fn balance(&self, user_id: Uuid) -> Result<u64> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT balance FROM accounts WHERE id = $1")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|(b,)| b as u64).unwrap_or(0))
    }

    async fn debit_processed(&self, debit_id: Uuid) -> Result<bool> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS( SELECT 1 FROM processed_debits WHERE id = $1 )")
                .bind(debit_id)
                .fetch_one(&self.pool)
                .await?;

        Ok(exists)
    }

    async fn mark_processed(
        &self,
        debit_id: Uuid,
        order_id: Uuid,
        user_id: Uuid,
        amount: u64,
    ) -> Result<()> {
        let amt = amount as i64;

        sqlx::query(
            "INSERT INTO processed_debits (id, order_id, user_id, amount)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(debit_id)
        .bind(order_id)
        .bind(user_id)
        .bind(amt)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn statement(&self, user_id: Uuid, since: DateTime<Utc>) -> Result<Vec<LedgerEntry>> {
        let credit_rows: Vec<(i64, DateTime<Utc>)> = sqlx::query_as(
            "SELECT amount, created FROM credits
             WHERE user_id = $1 AND created >= $2",
        )
        .bind(user_id)
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        let debit_rows: Vec<(Uuid, Uuid, i64, DateTime<Utc>)> = sqlx::query_as(
            "SELECT id, order_id, amount, created FROM processed_debits
             WHERE user_id = $1 AND created >= $2",
        )
        .bind(user_id)
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        let mut entries = Vec::with_capacity(credit_rows.len() + debit_rows.len());

        for (amount, created) in credit_rows {
            entries.push(LedgerEntry::Credit {
                amount: amount as u64,
                at: created,
            });
        }

        for (debit_id, order_id, amount, created) in debit_rows {
            entries.push(LedgerEntry::Debit {
                debit_id,
                order_id,
                amount: amount as u64,
                at: created,
            });
        }

        entries.sort_by(|a, b| b.at().cmp(&a.at()));

        Ok(entries)
    }
}
