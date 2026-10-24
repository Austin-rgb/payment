//! Event-stream wiring: subscribes the service to `payment.debit.request`.

use std::sync::Arc;

use async_trait::async_trait;
use ferrumec::event::EventError;
use ferrumec::{Event, EventStream, Subscriber};
use viewset::ApiError;

use crate::models::DebitRequest;
use crate::service::PaymentService;

/// Subscribe `service` to [`DebitRequest`] on `es`.
pub async fn register(
    service: Arc<PaymentService>,
    es: Arc<dyn EventStream>,
) -> Result<(), EventError> {
    PaymentSubscriber { service }.subscribe(es).await
}

struct PaymentSubscriber {
    service: Arc<PaymentService>,
}

#[async_trait]
impl Subscriber<DebitRequest> for PaymentSubscriber {
    /// Transient failures (database, cache, outbox) are returned as errors so an
    /// at-least-once stream (`NatsAloStream`) redelivers; handling is idempotent
    /// on `debit_id`, so redelivery is safe. Invalid requests can never succeed,
    /// so they are logged and dropped instead of being redelivered forever.
    async fn on_message(
        &self,
        event: Event<DebitRequest>,
        _subject: &str,
    ) -> Result<(), EventError> {
        let debit_id = event.payload.debit_id;
        match self.service.handle_debit_request(event.payload).await {
            Ok(()) => Ok(()),
            Err(ApiError::Validation(msg)) => {
                tracing::error!(%debit_id, "dropping invalid debit request: {msg}");
                Ok(())
            }
            Err(e) => {
                tracing::error!(%debit_id, error = %e, "debit request failed");
                Err(Box::new(e))
            }
        }
    }
}
