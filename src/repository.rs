//! Durable storage abstraction and its SQLite implementation.
//!
//! The repository handles the pieces of state that **must** survive a
//! process restart:
//! - account balances,
//! - the credit history (for statements),
//! - the processed-debit idempotency log, which also doubles as the debit
//!   history (for statements) since it already records amount, time, and
//!   order_id at the moment a debit settles.
//!
//! Events that need to reach a broker are handled separately by
//! [`crate::event_store`].
//!
//! # Schema additions
//!
//! This module assumes the following DDL exists alongside the existing
//! `accounts` table (the project's `schema` module is not part of this
//! review — add these wherever your migrations live):
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS credits (
//!     id         INTEGER PRIMARY KEY AUTOINCREMENT,
//!     user_id    TEXT    NOT NULL,
//!     amount     INTEGER NOT NULL,
//!     created_at TEXT    NOT NULL
//! );
//! CREATE INDEX IF NOT EXISTS idx_credits_user_time ON credits(user_id, created_at);
//!
//! CREATE TABLE IF NOT EXISTS processed_debits (
//!     debit_id   TEXT PRIMARY KEY,
//!     order_id   TEXT    NOT NULL,
//!     user_id    TEXT    NOT NULL,
//!     amount     INTEGER NOT NULL,
//!     created_at TEXT    NOT NULL
//! );
//! CREATE INDEX IF NOT EXISTS idx_processed_debits_user_time ON processed_debits(user_id, created_at);
//! ```
//!
//! If `processed_debits` already exists with just a `debit_id` primary key,
//! this is a breaking migration (new NOT NULL columns) — backfill or widen
//! with defaults as appropriate for your deployment.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
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
    /// must check [`debit_processed`] first.
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
    async fn statement(
        &self,
        user_id: Uuid,
        since: DateTime<Utc>,
    ) -> Result<Vec<LedgerEntry>>;
}

// ---------------------------------------------------------------------------
// SQLite implementation
// ---------------------------------------------------------------------------

/// [`PaymentRepository`] backed by a SQLite database via `sqlx`.
///
/// All queries use `sqlx::query` / `sqlx::query_as` (dynamic queries) rather
/// than the `query!` macro so that no compile-time database connection is
/// required.
pub struct SqliteRepository {
    pool: SqlitePool,
}

impl SqliteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PaymentRepository for SqliteRepository {
    async fn credit(&self, user_id: Uuid, amount: u64) -> Result<()> {
        let uid = user_id.to_string();
        let amt = amount as i64;
        let now = Utc::now();

        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "INSERT INTO accounts (user_id, balance) VALUES (?, ?)
             ON CONFLICT(user_id) DO UPDATE SET balance = balance + excluded.balance",
        )
        .bind(&uid)
        .bind(amt)
        .execute(&mut *tx)
        .await?;

        sqlx::query("INSERT INTO credits (user_id, amount, created_at) VALUES (?, ?, ?)")
            .bind(&uid)
            .bind(amt)
            .bind(now.to_rfc3339())
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn try_debit(&self, user_id: Uuid, amount: u64) -> Result<bool> {
        let uid = user_id.to_string();
        let amt = amount as i64;

        let mut tx = self.pool.begin().await?;

        // Read current balance inside the transaction.
        let row: Option<(i64,)> = sqlx::query_as("SELECT balance FROM accounts WHERE user_id = ?")
            .bind(&uid)
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

        sqlx::query("UPDATE accounts SET balance = balance - ? WHERE user_id = ?")
            .bind(amt)
            .bind(&uid)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(true)
    }

    async fn balance(&self, user_id: Uuid) -> Result<u64> {
        let uid = user_id.to_string();

        let row: Option<(i64,)> = sqlx::query_as("SELECT balance FROM accounts WHERE user_id = ?")
            .bind(&uid)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|(b,)| b as u64).unwrap_or(0))
    }

    async fn debit_processed(&self, debit_id: Uuid) -> Result<bool> {
        let did = debit_id.to_string();

        // SQLite returns 0 or 1 for EXISTS; fetch as i64 to avoid driver quirks.
        let row: (i64,) =
            sqlx::query_as("SELECT EXISTS( SELECT 1 FROM processed_debits WHERE debit_id = ? )")
                .bind(&did)
                .fetch_one(&self.pool)
                .await?;

        Ok(row.0 != 0)
    }

    async fn mark_processed(
        &self,
        debit_id: Uuid,
        order_id: Uuid,
        user_id: Uuid,
        amount: u64,
    ) -> Result<()> {
        let did = debit_id.to_string();
        let oid = order_id.to_string();
        let uid = user_id.to_string();
        let amt = amount as i64;
        let now = Utc::now();

        sqlx::query(
            "INSERT OR IGNORE INTO processed_debits
                 (debit_id, order_id, user_id, amount, created_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&did)
        .bind(&oid)
        .bind(&uid)
        .bind(amt)
        .bind(now.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn statement(&self, user_id: Uuid, since: DateTime<Utc>) -> Result<Vec<LedgerEntry>> {
        let uid = user_id.to_string();
        let since_str = since.to_rfc3339();

        let credit_rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT amount, created_at FROM credits
             WHERE user_id = ? AND created_at >= ?",
        )
        .bind(&uid)
        .bind(&since_str)
        .fetch_all(&self.pool)
        .await?;

        let debit_rows: Vec<(String, String, i64, String)> = sqlx::query_as(
            "SELECT debit_id, order_id, amount, created_at FROM processed_debits
             WHERE user_id = ? AND created_at >= ?",
        )
        .bind(&uid)
        .bind(&since_str)
        .fetch_all(&self.pool)
        .await?;

        let mut entries = Vec::with_capacity(credit_rows.len() + debit_rows.len());

        for (amount, created_at) in credit_rows {
            entries.push(LedgerEntry::Credit {
                amount: amount as u64,
                at: DateTime::parse_from_rfc3339(&created_at)?.with_timezone(&Utc),
            });
        }

        for (debit_id, order_id, amount, created_at) in debit_rows {
            entries.push(LedgerEntry::Debit {
                debit_id: Uuid::parse_str(&debit_id)?,
                order_id: Uuid::parse_str(&order_id)?,
                amount: amount as u64,
                at: DateTime::parse_from_rfc3339(&created_at)?.with_timezone(&Utc),
            });
        }

        entries.sort_by(|a, b| b.at().cmp(&a.at()));

        Ok(entries)
    }
}

