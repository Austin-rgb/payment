//! Data models shared across the payment service.

use chrono::{DateTime, Utc};
use event_stream::{Publishable, Subscribable};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// HTTP request bodies
// ---------------------------------------------------------------------------

/// JSON body for `POST /payment/credit`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditRequest {
    pub user_id: Uuid,
    pub amount: u64,
}

// ---------------------------------------------------------------------------
// Domain events (inbound)
// ---------------------------------------------------------------------------

/// Inbound event: a caller wants to debit a user's account.
///
/// `debit_id` is a caller-supplied idempotency key. Once a debit with this ID
/// has been processed (success **or** permanent failure), the ID is recorded in
/// `processed_debits` so that re-delivered broker messages are silently ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebitRequest {
    pub debit_id: Uuid,
    pub user_id: Uuid,
    pub amount: u64,
}

impl Subscribable for DebitRequest {
    const SUBJECT: &'static str = "payment.debit.request";
}

// ---------------------------------------------------------------------------
// Domain events (outbound)
// ---------------------------------------------------------------------------

/// Emitted on `payment.debit.success` when a debit completed successfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebitSuccess {
    pub debit_id: Uuid,
    pub user_id: Uuid,
    pub amount: u64,
}

impl Publishable for DebitSuccess {
    const SUBJECT: &'static str = "payment.debit.success";
}

/// Emitted on `payment.debit.failed` when a debit could not be fulfilled
/// within the configured `maximum_wait` window.
///
/// `balance` is the user's balance **at the exact instant the failure decision
/// was made**, giving downstream consumers a snapshot without requiring them to
/// re-query the service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebitFailed {
    pub debit_id: Uuid,
    pub user_id: Uuid,
    pub amount: u64,
    /// Balance at the instant of failure — not necessarily zero.
    pub balance: u64,
    pub reason: DebitFailureReason,
}

impl Publishable for DebitFailed {
    const SUBJECT: &'static str = "payment.debit.failed";
}

/// Why a deferred debit ultimately failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DebitFailureReason {
    /// The debit waited longer than the service's `maximum_wait` duration.
    MaximumWaitExceeded,
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// A debit that could not be satisfied immediately and is waiting for a credit
/// event to top up the account before its deadline.
///
/// Pending debits live **only in Moka** (see [`crate::pending_store`]). They
/// are deliberately non-durable: if the process restarts, pending debits are
/// lost and no `DebitFailed` event is emitted. The originating system must
/// handle the absence of a result (e.g. by re-sending after a timeout). This
/// keeps the implementation simple and avoids distributed locking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingDebit {
    pub debit_id: Uuid,
    pub user_id: Uuid,
    pub amount: u64,
    /// Absolute wall-clock deadline; once `Utc::now() >= expires_at` the debit
    /// is abandoned and a `DebitFailed` event is emitted.
    pub expires_at: DateTime<Utc>,
}

