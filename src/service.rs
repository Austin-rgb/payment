//! Core payment business logic.
//!
//! [`PaymentService`] is generic over its dependencies so that each can be
//! substituted in tests without spinning up a real database or broker.

use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use event_stream::{Event, EventMetaData, EventStream};
use tracing::{info, warn};

use crate::{
    models::{
        CreditRequest, DebitFailed, DebitFailureReason, DebitRequest, DebitSuccess, PendingDebit,
    },
    pending_store::PendingDebitStore,
    repository::PaymentRepository,
};

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// Orchestrates credits, deferred debits, and event emission.
///
/// All durable writes go through `repo`; ephemeral pending-debit state lives
/// in `pending`; outbound events are queued in `events` for a background
/// worker to forward to the broker.
pub struct PaymentService<R, P> {
    repo: Arc<R>,
    pending: Arc<P>,
    es: Arc<dyn EventStream>,
    /// Maximum time to wait for funds before abandoning a pending debit.
    maximum_wait: chrono::Duration,
}

impl<R, P> PaymentService<R, P>
where
    R: PaymentRepository,
    P: PendingDebitStore,
{
    pub fn new(
        repo: Arc<R>,
        pending: Arc<P>,
        es: Arc<dyn EventStream>,
        maximum_wait: chrono::Duration,
    ) -> Self {
        Self {
            repo,
            pending,
            es,
            maximum_wait,
        }
    }

    // -----------------------------------------------------------------------
    // Credit flow
    // -----------------------------------------------------------------------

    /// Credit `req.user_id`'s account and retry any pending debits for them.
    ///
    /// Steps for each existing pending debit:
    /// 1. **Expired** (`now >= expires_at`): read the balance, discard the
    ///    pending debit, emit `payment.debit.failed`.
    /// 2. **Live, retry succeeds**: remove the pending debit, mark processed,
    ///    emit `payment.debit.success`.
    /// 3. **Live, retry fails** (still insufficient funds): leave in place.
    pub async fn credit(&self, req: CreditRequest) -> Result<()> {
        info!(
            user_id = %req.user_id,
            amount  = req.amount,
            "credit received"
        );

        self.repo.credit(req.user_id, req.amount).await?;

        let pending_debits = self.pending.pending_for_user(req.user_id).await;

        for debit in pending_debits {
            let now = Utc::now();

            if now >= debit.expires_at {
                self.handle_expired_debit(&debit).await?;
            } else {
                self.retry_pending_debit(&debit).await?;
            }
        }

        Ok(())
    }

    /// Emit `payment.debit.failed` and discard an expired pending debit.
    async fn handle_expired_debit(&self, debit: &PendingDebit) -> Result<()> {
        warn!(
            debit_id   = %debit.debit_id,
            user_id    = %debit.user_id,
            expires_at = %debit.expires_at,
            "debit failure due to expiry"
        );

        let balance = self.repo.balance(debit.user_id).await?;
        self.pending.remove(debit.debit_id).await;

        let failed = DebitFailed {
            debit_id: debit.debit_id,
            user_id: debit.user_id,
            amount: debit.amount,
            balance,
            reason: DebitFailureReason::MaximumWaitExceeded,
        };

        let emd = EventMetaData::new("payment");
        let event = Event::new(emd, failed);
        if let Err(e) = event.publish(self.es.clone()).await {
            tracing::error!("Failed to publish event: {e}");
        };

        Ok(())
    }

    /// Attempt one debit retry for a live pending debit.
    async fn retry_pending_debit(&self, debit: &PendingDebit) -> Result<()> {
        let succeeded = self.repo.try_debit(debit.user_id, debit.amount).await?;

        if succeeded {
            info!(
                debit_id = %debit.debit_id,
                user_id  = %debit.user_id,
                amount   = debit.amount,
                "debit success (resolved from pending)"
            );
            self.pending.remove(debit.debit_id).await;
            self.repo.mark_processed(debit.debit_id).await?;

            let success = DebitSuccess {
                debit_id: debit.debit_id,
                user_id: debit.user_id,
                amount: debit.amount,
            };
            let emd = EventMetaData::new("payment");
            let event = Event::new(emd, success);
            if let Err(e) = event.publish(self.es.clone()).await {
                tracing::error!("Failed to publish event: {e}");
            };
        }
        // If still insufficient leave the pending debit in place.

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Debit request flow
    // -----------------------------------------------------------------------

    /// Handle an inbound `DebitRequest` event.
    ///
    /// Already-processed debit IDs are ignored to make the handler idempotent
    /// against re-delivered broker messages. If funds are insufficient the
    /// debit is parked as a [`PendingDebit`] for deferred retry on the next
    /// credit.
    pub async fn handle_debit_request(&self, req: DebitRequest) -> Result<()> {
        info!(
            debit_id = %req.debit_id,
            user_id  = %req.user_id,
            amount   = req.amount,
            "debit request received"
        );

        // Idempotency guard.
        if self.repo.debit_processed(req.debit_id).await? {
            info!(debit_id = %req.debit_id, "debit already processed, skipping");
            return Ok(());
        }

        let succeeded = self.repo.try_debit(req.user_id, req.amount).await?;

        if succeeded {
            info!(
                debit_id = %req.debit_id,
                user_id  = %req.user_id,
                amount   = req.amount,
                "debit success"
            );
            self.repo.mark_processed(req.debit_id).await?;

            let success = DebitSuccess {
                debit_id: req.debit_id,
                user_id: req.user_id,
                amount: req.amount,
            };

            let emd = EventMetaData::new("payment");
            let event = Event::new(emd, success);
            if let Err(e) = event.publish(self.es.clone()).await {
                tracing::error!("Failed to publish event: {e}");
            };
        } else {
            // Insufficient funds — park for deferred retry.
            let expires_at = Utc::now() + self.maximum_wait;
            let pending = PendingDebit {
                debit_id: req.debit_id,
                user_id: req.user_id,
                amount: req.amount,
                expires_at,
            };
            info!(
                debit_id   = %req.debit_id,
                user_id    = %req.user_id,
                expires_at = %expires_at,
                "pending debit queued"
            );
            self.pending.add(pending).await;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helper: shared application state type alias
// ---------------------------------------------------------------------------

/// Convenience alias for the concrete service used in Actix handlers.
///
/// Replace the type parameters with your chosen implementations.
pub type SharedService<R, P> = Arc<PaymentService<R, P>>;
