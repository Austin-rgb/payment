//! Durable ledger operations (Postgres via `sqlx`).
//!
//! Everything that must survive a restart lives here: account balances, the
//! credit history and the processed-debit log (which doubles as the debit
//! history for statements).
//!
//! These are plain functions over a pool/transaction rather than a trait: the
//! atomic operations need a caller-owned transaction so that the matching
//! domain event can be written to the ferrumec `Outbox` in the *same*
//! transaction (see [`crate::service::PaymentService`]).
//!
//! Queries use `sqlx::query*` (not the `query!` macros) so building does not
//! need a live database. The schema is in `migrations/`.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;
use viewset::ApiError;

use crate::models::LedgerEntry;

pub type Result<T> = std::result::Result<T, ApiError>;

/// What happened when a debit was attempted inside a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebitOutcome {
    /// Balance deducted and the debit recorded. The caller must commit.
    Settled,
    /// The balance was too low (or the account does not exist). The caller
    /// must roll back.
    InsufficientFunds,
    /// This `debit_id` already settled earlier. The caller must roll back.
    AlreadyProcessed,
}

/// `u64` → `i64` without wrapping into a negative number.
fn to_i64(amount: u64) -> Result<i64> {
    i64::try_from(amount).map_err(|_| ApiError::Validation("amount is too large".into()))
}

/// Add `amount` to `user_id`'s balance (creating the account on first credit)
/// and append a `credits` row, atomically.
pub async fn credit(pool: &PgPool, user_id: Uuid, amount: u64) -> Result<()> {
    let amt = to_i64(amount)?;
    let mut tx = pool.begin().await?;

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

/// Try to settle a debit inside `tx`: check idempotency, check funds, deduct
/// the balance and record the debit — all under one row lock on the account,
/// so concurrent deliveries of the same debit (or competing debits on the same
/// account) are serialised.
///
/// Only [`DebitOutcome::Settled`] leaves anything to commit; for the other
/// outcomes the caller **must** roll back (the processed-debit row has already
/// been inserted at that point).
pub async fn settle_debit(
    tx: &mut Transaction<'_, Postgres>,
    debit_id: Uuid,
    order_id: Uuid,
    user_id: Uuid,
    amount: u64,
) -> Result<DebitOutcome> {
    let amt = to_i64(amount)?;

    // Row lock for the rest of the transaction: two debits can't both read
    // the same balance and both succeed under READ COMMITTED.
    let balance: Option<i64> =
        sqlx::query_scalar("SELECT balance FROM accounts WHERE id = $1 FOR UPDATE")
            .bind(user_id)
            .fetch_optional(&mut **tx)
            .await?;
    let Some(balance) = balance else {
        return Ok(DebitOutcome::InsufficientFunds);
    };

    // Idempotency first: a redelivered, already-settled debit must not be
    // mistaken for "insufficient funds" just because the balance has dropped.
    let inserted = sqlx::query(
        "INSERT INTO processed_debits (id, order_id, user_id, amount)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(debit_id)
    .bind(order_id)
    .bind(user_id)
    .bind(amt)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if inserted == 0 {
        return Ok(DebitOutcome::AlreadyProcessed);
    }

    if balance < amt {
        return Ok(DebitOutcome::InsufficientFunds);
    }

    sqlx::query("UPDATE accounts SET balance = balance - $1 WHERE id = $2")
        .bind(amt)
        .bind(user_id)
        .execute(&mut **tx)
        .await?;

    Ok(DebitOutcome::Settled)
}

/// Current balance for `user_id`, or `0` if no account exists.
pub async fn balance(pool: &PgPool, user_id: Uuid) -> Result<u64> {
    let row: Option<i64> = sqlx::query_scalar("SELECT balance FROM accounts WHERE id = $1")
        .bind(user_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|b| b as u64).unwrap_or(0))
}

/// Credits and settled debits for `user_id` at or after `since`, newest first.
pub async fn statement(
    pool: &PgPool,
    user_id: Uuid,
    since: DateTime<Utc>,
) -> Result<Vec<LedgerEntry>> {
    let credit_rows: Vec<(i64, DateTime<Utc>)> =
        sqlx::query_as("SELECT amount, created FROM credits WHERE user_id = $1 AND created >= $2")
            .bind(user_id)
            .bind(since)
            .fetch_all(pool)
            .await?;

    let debit_rows: Vec<(Uuid, Uuid, i64, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id, order_id, amount, created FROM processed_debits
         WHERE user_id = $1 AND created >= $2",
    )
    .bind(user_id)
    .bind(since)
    .fetch_all(pool)
    .await?;

    let mut entries = Vec::with_capacity(credit_rows.len() + debit_rows.len());
    entries.extend(
        credit_rows
            .into_iter()
            .map(|(amount, at)| LedgerEntry::Credit {
                amount: amount as u64,
                at,
            }),
    );
    entries.extend(
        debit_rows
            .into_iter()
            .map(|(debit_id, order_id, amount, at)| LedgerEntry::Debit {
                debit_id,
                order_id,
                amount: amount as u64,
                at,
            }),
    );
    entries.sort_by_key(|e| std::cmp::Reverse(e.at()));
    Ok(entries)
}
