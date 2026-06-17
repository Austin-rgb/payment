//! Event broker abstractions and subscription wiring.
//!
//! No concrete broker is implemented here. Bring your own NATS / Redis /
//! Kafka client and implement [`EventPublisher`] and [`EventSubscriber`] for
//! it; then call [`register_subscriptions`] during startup.

use std::sync::Arc;

use event_stream::{EventStream, Subscriber};

use crate::{
    models::DebitRequest, pending_store::PendingDebitStore, repository::PaymentRepository,
    service::PaymentService,
};
// ---------------------------------------------------------------------------
// Subscription wiring
// ---------------------------------------------------------------------------

/// Subscribe `service` to the `payment.debit.request` topic via `subscriber`.
///
/// Each incoming event is deserialised as a [`DebitRequest`] and forwarded to
/// [`PaymentService::handle_debit_request`]. Errors are logged and the event
/// is dropped (no broker-level retry at this layer — rely on the broker's own
/// redelivery policy and the service's idempotency guard).
pub async fn register_subscriptions<R, P>(
    service: Arc<PaymentService<R, P>>,
    es: Arc<dyn EventStream>,
) where
    R: PaymentRepository,
    P: PendingDebitStore,
{
    let subscriber = PaymentSubscriber { service };
    if let Err(e) = subscriber.subscribe(es).await {
        tracing::error!("Failed to subscribe to event stream: {e}");
    };
}

struct PaymentSubscriber<R, P> {
    service: Arc<PaymentService<R, P>>,
}

#[async_trait::async_trait]
impl<R: PaymentRepository + Send + Sync + 'static, P: PendingDebitStore + Send + Sync + 'static>
    Subscriber<DebitRequest> for PaymentSubscriber<R, P>
{
    async fn on_message(&self, event: event_stream::Event<DebitRequest>, _subject: &str) {
        if let Err(e) = self
            .service
            .clone()
            .handle_debit_request(event.payload)
            .await
        {
            tracing::error!("Error in handling debit request: {e}");
        };
    }
}
