//! Ephemeral outbox replacement: events to be published are queued in a Moka
//! cache instead of a database table.
//!
//! # Why Moka instead of a database outbox?
//!
//! The classic outbox pattern persists events to a DB table so they survive
//! crashes. Here we accept a simpler trade-off:
//!
//! - **Durable facts** (balances, processed-debit IDs) remain in SQLite.
//! - **Outbound events** are best-effort: if the process dies before the
//!   background worker drains the cache the events are lost. Downstream
//!   consumers that need exactly-once delivery must implement their own
//!   idempotency (which they should anyway).
//!
//! The benefits are: no additional DB writes per credit/debit operation,
//! automatic memory bounding via Moka's capacity limit, and no need for a
//! `published` flag or cleanup job.
//!
//! A background worker calls [`MokaEventStore::drain`] in a loop and publishes
//! whatever it finds to the real broker.

use crate::models::PendingEvent;

use async_trait::async_trait;
use moka::future::Cache;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Queue and drain outbound domain events.
#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    /// Enqueue an event for later publishing.
    async fn enqueue(&self, topic: String, payload: serde_json::Value);

    /// Remove and return all currently queued events.
    ///
    /// Called by the background worker that forwards events to the broker.
    async fn drain(&self) -> Vec<PendingEvent>;
}

// ---------------------------------------------------------------------------
// Moka implementation
// ---------------------------------------------------------------------------

/// [`EventStore`] backed by a Moka cache.
///
/// Each entry is keyed by a freshly generated [`Uuid`] so that multiple events
/// on the same topic can coexist without overwriting each other.
pub struct MokaEventStore {
    cache: Cache<Uuid, PendingEvent>,
}

impl MokaEventStore {
    /// Create a new event store capped at `max_capacity` entries.
    pub fn new(max_capacity: u64) -> Self {
        Self {
            cache: Cache::new(max_capacity),
        }
    }
}

#[async_trait]
impl EventStore for MokaEventStore {
    async fn enqueue(&self, topic: String, payload: serde_json::Value) {
        let id = Uuid::new_v4();
        let event = PendingEvent { id, topic, payload };
        self.cache.insert(id, event).await;
    }

    async fn drain(&self) -> Vec<PendingEvent> {
        // Snapshot all current keys, then invalidate each one individually.
        // Moka does not expose a transactional drain, so there is a small
        // window where a freshly inserted event could be missed — acceptable
        // for best-effort delivery.
        let entries: Vec<PendingEvent> = self.cache.iter().map(|(_, v)| v.clone()).collect();

        for event in &entries {
            self.cache.invalidate(&event.id).await;
        }

        entries
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn enqueue_then_drain() {
        let store = MokaEventStore::new(64);

        store
            .enqueue("payment.debit.success".into(), json!({"amount": 100}))
            .await;
        store
            .enqueue("payment.debit.failed".into(), json!({"amount": 50}))
            .await;

        let drained = store.drain().await;
        assert_eq!(drained.len(), 2);

        // Cache should be empty after drain.
        assert!(store.drain().await.is_empty());
    }

    #[tokio::test]
    async fn drain_is_cleared() {
        let store = MokaEventStore::new(64);
        store.enqueue("topic".into(), json!({})).await;

        let first = store.drain().await;
        let second = store.drain().await;

        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
    }
}
