//! Data models shared across the payment service: HTTP bodies, the statement
//! types and the domain events carried on the ferrumec event stream.

use chrono::{DateTime, Utc};
use ferrumec::EventType;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

/// JSON body for `POST /{namespace}/credit`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditRequest {
    pub user_id: Uuid,
    pub amount: u64,
}

/// Query string for `GET /{namespace}/statement/{user_id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementQuery {
    /// Only entries at or after this timestamp are returned (RFC 3339).
    pub since: DateTime<Utc>,
}

/// One entry in a user's balance-affecting history. Pending (parked) debits
/// are deliberately excluded since they have not settled.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LedgerEntry {
    Credit {
        amount: u64,
        at: DateTime<Utc>,
    },
    Debit {
        debit_id: Uuid,
        order_id: Uuid,
        amount: u64,
        at: DateTime<Utc>,
    },
}

impl LedgerEntry {
    /// Timestamp used to sort entries, newest first.
    pub fn at(&self) -> DateTime<Utc> {
        match self {
            LedgerEntry::Credit { at, .. } | LedgerEntry::Debit { at, .. } => *at,
        }
    }
}

/// Response body for `GET /{namespace}/statement/{user_id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResponse {
    pub user_id: Uuid,
    /// Entries sorted by `at` descending (most recent first).
    pub entries: Vec<LedgerEntry>,
}

// ---------------------------------------------------------------------------
// Events (inbound)
// ---------------------------------------------------------------------------

/// Inbound event: a caller wants to debit a user's account.
///
/// `debit_id` is a caller-supplied idempotency key. Once a debit with this id
/// has settled it is recorded in `processed_debits`, so re-delivered messages
/// are recognised and ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebitRequest {
    pub debit_id: Uuid,
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub amount: u64,
}

impl EventType for DebitRequest {
    const SUBJECT: &'static str = "payment.debit.request";
}

// ---------------------------------------------------------------------------
// Events (outbound)
// ---------------------------------------------------------------------------

/// Emitted on `payment.debit.success` when a debit settled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebitSuccess {
    pub debit_id: Uuid,
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub amount: u64,
}

impl EventType for DebitSuccess {
    const SUBJECT: &'static str = "payment.debit.success";
}

/// Emitted on `payment.debit.failed` when a parked debit could not be
/// fulfilled within the configured maximum wait.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebitFailed {
    pub debit_id: Uuid,
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub amount: u64,
    /// Balance at the instant the failure was decided — not necessarily zero.
    pub balance: u64,
    pub reason: DebitFailureReason,
}

impl EventType for DebitFailed {
    const SUBJECT: &'static str = "payment.debit.failed";
}

/// Why a deferred debit ultimately failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DebitFailureReason {
    /// The debit waited longer than the service's maximum wait.
    MaximumWaitExceeded,
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// A debit that could not be satisfied immediately and is waiting for a credit
/// to top up the account before its deadline.
///
/// Pending debits live **only in the cache** (see [`crate::pending_store`]),
/// so they are non-durable by design: if the cache is lost, those debits
/// vanish without a `DebitFailed` event and the originating system must time
/// out and retry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingDebit {
    pub debit_id: Uuid,
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub amount: u64,
    /// Wall-clock deadline; once reached the debit is abandoned and a
    /// `DebitFailed` event is emitted on the next credit for this user.
    pub expires_at: DateTime<Utc>,
}
