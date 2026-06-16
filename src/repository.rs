//! Durable storage abstraction and its SQLite implementation.
//!
//! The repository handles the three pieces of state that **must** survive a
//! process restart:
//! - account balances,
//! - the processed-debit idempotency log,
//!
//! Events that need to reach a broker are handled separately by
//! [`crate::event_store`].

use anyhow::Result;
use async_trait::async_trait;
use sqlx::SqlitePool;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Persistent storage operations for the payment service.
#[async_trait]
pub trait PaymentRepository: Send + Sync + 'static {
    /// Add `amount` to `user_id`'s balance, creating the account row if it
    /// does not yet exist.
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

    /// Record `debit_id` as processed (idempotent — safe to call twice).
    async fn mark_processed(&self, debit_id: Uuid) -> Result<()>;
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

        sqlx::query(
            "INSERT INTO accounts (user_id, balance) VALUES (?, ?)
             ON CONFLICT(user_id) DO UPDATE SET balance = balance + excluded.balance",
        )
        .bind(&uid)
        .bind(amt)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn try_debit(&self, user_id: Uuid, amount: u64) -> Result<bool> {
        let uid = user_id.to_string();
        let amt = amount as i64;

        let mut tx = self.pool.begin().await?;

        // Read current balance inside the transaction.
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT balance FROM accounts WHERE user_id = ?")
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

        let row: Option<(i64,)> =
            sqlx::query_as("SELECT balance FROM accounts WHERE user_id = ?")
                .bind(&uid)
                .fetch_optional(&self.pool)
                .await?;

        Ok(row.map(|(b,)| b as u64).unwrap_or(0))
    }

    async fn debit_processed(&self, debit_id: Uuid) -> Result<bool> {
        let did = debit_id.to_string();

        // SQLite returns 0 or 1 for EXISTS; fetch as i64 to avoid driver quirks.
        let row: (i64,) = sqlx::query_as(
            "SELECT EXISTS( SELECT 1 FROM processed_debits WHERE debit_id = ? )",
        )
        .bind(&did)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.0 != 0)
    }

    async fn mark_processed(&self, debit_id: Uuid) -> Result<()> {
        let did = debit_id.to_string();

        sqlx::query("INSERT OR IGNORE INTO processed_debits (debit_id) VALUES (?)")
            .bind(&did)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

