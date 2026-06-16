//! Event broker abstractions and subscription wiring.
//!
//! No concrete broker is implemented here. Bring your own NATS / Redis /
//! Kafka client and implement [`EventPublisher`] and [`EventSubscriber`] for
//! it; then call [`register_subscriptions`] during startup.

use std::sync::Arc;

use futures::future::BoxFuture;
use serde::Serialize;
use tracing::error;

use crate::{
    event_store::EventStore,
    models::DebitRequest,
    pending_store::PendingDebitStore,
    repository::PaymentRepository,
    service::PaymentService,
};

// ---------------------------------------------------------------------------
// Event publisher trait
// ---------------------------------------------------------------------------

/// Publish a serialisable payload to a named topic.
///
/// Implementations forward to the underlying broker (NATS, Redis Streams,
/// Kafka, etc.). The [`crate::event_store::EventStore`] feeds this trait from
/// a background worker loop.
#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync + 'static {
    async fn publish<T>(&self, topic: &str, payload: &T)
    where
        T: Serialize + Send + Sync;
}

// ---------------------------------------------------------------------------
// Event subscriber trait
// ---------------------------------------------------------------------------

/// Boxed async handler function for incoming [`DebitRequest`] events.
pub type DebitRequestHandler =
    Box<dyn Fn(DebitRequest) -> BoxFuture<'static, ()> + Send + Sync>;

/// Register handlers for incoming broker events.
#[async_trait::async_trait]
pub trait EventSubscriber: Send + Sync + 'static {
    /// Register `handler` to be called for every `payment.debit.request`
    /// event delivered by the broker.
    async fn on_debit_request(&self, handler: DebitRequestHandler);
}

// ---------------------------------------------------------------------------
// Subscription wiring
// ---------------------------------------------------------------------------

/// Subscribe `service` to the `payment.debit.request` topic via `subscriber`.
///
/// Each incoming event is deserialised as a [`DebitRequest`] and forwarded to
/// [`PaymentService::handle_debit_request`]. Errors are logged and the event
/// is dropped (no broker-level retry at this layer — rely on the broker's own
/// redelivery policy and the service's idempotency guard).
pub async fn register_subscriptions<R, P, E, S>(
    service:    Arc<PaymentService<R, P, E>>,
    subscriber: Arc<S>,
)
where
    R: PaymentRepository,
    P: PendingDebitStore,
    E: EventStore,
    S: EventSubscriber,
{
    let svc = service.clone();

    subscriber
        .on_debit_request(Box::new(move |req: DebitRequest| {
            let svc = svc.clone();
            Box::pin(async move {
                if let Err(e) = svc.handle_debit_request(req).await {
                    error!(error = %e, "error handling debit request");
                }
            })
        }))
        .await;
}

