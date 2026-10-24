//! Core payment business logic.
//!
//! [`PaymentService`] orchestrates credits, deferred debits and event
//! emission. Durable state goes through [`crate::ledger`]; parked debits live
//! in a [`PendingDebitStore`]. Outbound events are written to ferrumec's
//! transactional [`Outbox`] **in the same database transaction** as the ledger
//! change they describe, so `payment.debit.success` is published if and only if
//! the debit really settled (at-least-once; consumers dedupe on
//! `metadata.event_id`).

use std::sync::Arc;

use chrono::Utc;
use ferrumec::Event;
use ferrumec::event::Outbox;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;
use viewset::ApiError;

use crate::ledger::{self, DebitOutcome};
use crate::models::{
    CreditRequest, DebitFailed, DebitFailureReason, DebitRequest, DebitSuccess, LedgerEntry,
    PendingDebit,
};
use crate::pending_store::PendingDebitStore;

type Result<T> = std::result::Result<T, ApiError>;

/// Value of the `producer` field on every event this service emits.
const PRODUCER: &str = "payment";

pub struct PaymentService {
    pool: PgPool,
    outbox: Outbox,
    pending: Arc<dyn PendingDebitStore>,
    /// Maximum time a debit waits for funds before it is abandoned.
    maximum_wait: chrono::Duration,
}

fn outbox_err(e: ferrumec::event::EventError) -> ApiError {
    ApiError::Internal(format!("outbox: {e}"))
}

impl PaymentService {
    pub fn new(
        pool: PgPool,
        outbox: Outbox,
        pending: Arc<dyn PendingDebitStore>,
        maximum_wait: chrono::Duration,
    ) -> Self {
        Self {
            pool,
            outbox,
            pending,
            maximum_wait,
        }
    }

    // -----------------------------------------------------------------------
    // Credit flow
    // -----------------------------------------------------------------------

    /// Credit `req.user_id`'s account, then give that user's parked debits a
    /// chance to settle (or expire).
    ///
    /// Once the credit has committed this returns `Ok` even if processing the
    /// parked debits fails: reporting an error for money that *was* credited
    /// would invite the caller to retry and double-credit. Parked debits that
    /// could not be processed are simply picked up on the user's next credit.
    pub async fn credit(&self, req: CreditRequest) -> Result<()> {
        info!(user_id = %req.user_id, amount = req.amount, "credit received");

        ledger::credit(&self.pool, req.user_id, req.amount).await?;

        if let Err(e) = self.process_pending(req.user_id).await {
            error!(user_id = %req.user_id, error = %e, "processing pending debits failed after credit");
        }
        Ok(())
    }

    async fn process_pending(&self, user_id: Uuid) -> Result<()> {
        for debit in self.pending.pending_for_user(user_id).await? {
            if Utc::now() >= debit.expires_at {
                self.expire_debit(&debit).await?;
            } else {
                self.retry_debit(&debit).await?;
            }
        }
        Ok(())
    }

    /// Emit `payment.debit.failed` for an expired parked debit and drop it.
    ///
    /// The event is committed before the debit is removed from the store, so a
    /// crash in between re-emits the event (at-least-once) rather than losing it.
    async fn expire_debit(&self, debit: &PendingDebit) -> Result<()> {
        warn!(
            debit_id = %debit.debit_id,
            user_id = %debit.user_id,
            expires_at = %debit.expires_at,
            "debit failed: maximum wait exceeded"
        );

        let balance = ledger::balance(&self.pool, debit.user_id).await?;
        let failed = DebitFailed {
            debit_id: debit.debit_id,
            order_id: debit.order_id,
            user_id: debit.user_id,
            amount: debit.amount,
            balance,
            reason: DebitFailureReason::MaximumWaitExceeded,
        };

        let mut tx = self.pool.begin().await?;
        self.outbox
            .push(&Event::new(failed).with_producer(PRODUCER), &mut tx)
            .await
            .map_err(outbox_err)?;
        tx.commit().await?;

        self.pending.remove(debit.debit_id).await
    }

    /// One retry for a live parked debit; still-insufficient debits stay parked.
    async fn retry_debit(&self, debit: &PendingDebit) -> Result<()> {
        let outcome = self
            .settle(debit.debit_id, debit.order_id, debit.user_id, debit.amount)
            .await?;
        match outcome {
            DebitOutcome::Settled => {
                info!(debit_id = %debit.debit_id, user_id = %debit.user_id, "debit settled (from pending)");
                self.pending.remove(debit.debit_id).await
            }
            // Settled earlier but never removed from the store (e.g. a crash
            // between commit and removal): just clean up.
            DebitOutcome::AlreadyProcessed => self.pending.remove(debit.debit_id).await,
            DebitOutcome::InsufficientFunds => Ok(()),
        }
    }

    // -----------------------------------------------------------------------
    // Debit request flow
    // -----------------------------------------------------------------------

    /// Handle an inbound [`DebitRequest`].
    ///
    /// Idempotent against re-delivery: an already-settled `debit_id` is
    /// ignored. If funds are insufficient the debit is parked for deferred
    /// retry on the user's next credit.
    pub async fn handle_debit_request(&self, req: DebitRequest) -> Result<()> {
        info!(
            debit_id = %req.debit_id,
            user_id = %req.user_id,
            amount = req.amount,
            "debit request received"
        );

        match self
            .settle(req.debit_id, req.order_id, req.user_id, req.amount)
            .await?
        {
            DebitOutcome::Settled => {
                info!(debit_id = %req.debit_id, "debit settled");
            }
            DebitOutcome::AlreadyProcessed => {
                info!(debit_id = %req.debit_id, "debit already processed, skipping");
            }
            DebitOutcome::InsufficientFunds => {
                let expires_at = Utc::now() + self.maximum_wait;
                info!(
                    debit_id = %req.debit_id,
                    user_id = %req.user_id,
                    %expires_at,
                    "insufficient funds, debit parked"
                );
                self.pending
                    .add(PendingDebit {
                        debit_id: req.debit_id,
                        order_id: req.order_id,
                        user_id: req.user_id,
                        amount: req.amount,
                        expires_at,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    /// Attempt a debit and, if it settles, write `payment.debit.success` to the
    /// outbox in the same transaction. Any other outcome rolls back.
    async fn settle(
        &self,
        debit_id: Uuid,
        order_id: Uuid,
        user_id: Uuid,
        amount: u64,
    ) -> Result<DebitOutcome> {
        let mut tx = self.pool.begin().await?;
        let outcome = ledger::settle_debit(&mut tx, debit_id, order_id, user_id, amount).await?;

        if outcome == DebitOutcome::Settled {
            let success = DebitSuccess {
                debit_id,
                order_id,
                user_id,
                amount,
            };
            self.outbox
                .push(&Event::new(success).with_producer(PRODUCER), &mut tx)
                .await
                .map_err(outbox_err)?;
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        Ok(outcome)
    }

    // -----------------------------------------------------------------------
    // Statement
    // -----------------------------------------------------------------------

    /// `user_id`'s credits and settled debits since `since`, newest first.
    pub async fn statement(
        &self,
        user_id: Uuid,
        since: chrono::DateTime<Utc>,
    ) -> Result<Vec<LedgerEntry>> {
        ledger::statement(&self.pool, user_id, since).await
    }
}
