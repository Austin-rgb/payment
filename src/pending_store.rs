//! In-memory store for debits that are waiting for sufficient funds.
//!
//! Pending debits are intentionally **not** durable. If the process restarts
//! while debits are pending those debits are lost silently — no `DebitFailed`
//! event is emitted. This is an explicit design trade-off: it keeps the
//! implementation free of distributed coordination while placing the
//! responsibility for timeout handling on the originating system.
//!
//! Two caches are maintained to avoid a full-scan on per-user lookup:
//!
//! ```text
//! debits:     debit_id  →  PendingDebit
//! user_index: user_id   →  Vec<Uuid>   (debit IDs for that user)
//! ```
//!
//! Unlike the previous implementation, this store no longer constructs
//! [`moka::future::Cache`] directly. Instead both caches are obtained from a
//! [`crate::CacheFactory`] — the same abstraction `contributions` uses for
//! its item/list caches — via [`actixutils::Store`]. This decouples
//! `PendingDebitStore` from any particular cache backend: swapping the
//! factory implementation passed to [`crate::Module::new`] (e.g. from a
//! Moka-backed one to a Redis-backed one) changes where pending debits live
//! without touching this module at all.

use crate::{CacheFactory, models::PendingDebit};

use actix_web::Error;
use actixutils::Store;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait PendingDebitStore: Send + Sync + 'static {
    /// Park a debit for deferred retry.
    async fn add(&self, pending: PendingDebit) -> Result<(), Error>;

    /// Remove a debit (called after success or expiry).
    async fn remove(&self, debit_id: Uuid) -> Result<(), Error>;

    /// Return all pending debits currently associated with `user_id`.
    async fn pending_for_user(&self, user_id: Uuid) -> Result<Vec<PendingDebit>, Error>;
}

// ---------------------------------------------------------------------------
// Cache-factory-backed implementation
// ---------------------------------------------------------------------------

/// [`PendingDebitStore`] backed by two caches obtained from a
/// [`CacheFactory`] — named `"pending_debits"` and
/// `"pending_debit_user_index"` respectively.
///
/// The pending-debit TTL passed to the factory is deliberately generous
/// (see [`CacheBackedPendingDebitStore::new`]): a pending debit's *real*
/// deadline is [`PendingDebit::expires_at`], enforced by
/// [`crate::service::PaymentService`] on every credit; the cache TTL here is
/// only a backstop against entries that never get revisited (e.g. a user who
/// never receives another credit).
pub struct CacheBackedPendingDebitStore {
    /// Primary lookup: `debit_id → PendingDebit`.
    debits: Arc<dyn Store<Uuid, PendingDebit>>,
    /// Secondary index: `user_id → Vec<debit_id>`.
    ///
    /// Maintained in sync with `debits` so that [`Self::pending_for_user`]
    /// never needs to iterate all entries.
    user_index: Arc<dyn Store<Uuid, Vec<Uuid>>>,
}

impl CacheBackedPendingDebitStore {
    /// Create a new store, requesting its two named caches from `cf`.
    ///
    /// `ttl` is the backstop expiry described above; it should comfortably
    /// exceed [`crate::service::PaymentService`]'s configured
    /// `maximum_wait`, since a pending debit that's cache-evicted before its
    /// `expires_at` is reached is silently lost (same failure mode as a
    /// process restart — no `DebitFailed` is emitted).
    pub fn new<Cf: CacheFactory>(cf: &Cf, ttl: Duration) -> Self {
        Self {
            debits: cf.new_cache("pending_debits", ttl),
            user_index: cf.new_cache("pending_debit_user_index", ttl),
        }
    }
}

#[async_trait]
impl PendingDebitStore for CacheBackedPendingDebitStore {
    async fn add(&self, pending: PendingDebit) -> Result<(), Error> {
        let debit_id = pending.debit_id;
        let user_id = pending.user_id;

        self.debits.set(&debit_id, pending).await?;

        // Append debit_id to the user's index list.
        let mut ids = self.user_index.get(&user_id).await?.unwrap_or_default();

        if !ids.contains(&debit_id) {
            ids.push(debit_id);
        }
        self.user_index.set(&user_id, ids).await?;
        Ok(())
    }

    async fn remove(&self, debit_id: Uuid) -> Result<(), Error> {
        // Look up the owning user before evicting from the primary cache.
        let maybe_pending = self.debits.get(&debit_id).await?;

        if let Some(pending) = maybe_pending {
            let user_id = pending.user_id;
            self.debits.delete(&debit_id).await?;

            // Remove debit_id from the user's index list.
            let maybe_ids = self.user_index.get(&user_id).await?;

            if let Some(mut ids) = maybe_ids {
                ids.retain(|id| *id != debit_id);
                if ids.is_empty() {
                    self.user_index.delete(&user_id).await?;
                } else {
                    self.user_index.set(&user_id, ids).await?;
                }
            }
        }
        Ok(())
    }

    async fn pending_for_user(&self, user_id: Uuid) -> Result<Vec<PendingDebit>, Error> {
        let ids = match self.user_index.get(&user_id).await? {
            Some(ids) => ids,
            None => return Ok(vec![]),
        };

        let mut result = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some(pending) = self.debits.get(id).await? {
                result.push(pending);
            }
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MokaCacheFactory;
    use chrono::Utc;

    fn make_pending(debit_id: Uuid, user_id: Uuid, amount: u64) -> PendingDebit {
        PendingDebit {
            debit_id,
            order_id: Uuid::new_v4(),
            user_id,
            amount,
            expires_at: Utc::now() + chrono::Duration::seconds(60),
        }
    }

    fn make_store() -> CacheBackedPendingDebitStore {
        CacheBackedPendingDebitStore::new(&MokaCacheFactory::default(), Duration::from_secs(3600))
    }

    #[tokio::test]
    async fn add_and_lookup() {
        let store = make_store();
        let uid = Uuid::new_v4();
        let did = Uuid::new_v4();

        store.add(make_pending(did, uid, 100)).await;

        let pending = store.pending_for_user(uid).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].debit_id, did);
    }

    #[tokio::test]
    async fn remove_cleans_index() {
        let store = make_store();
        let uid = Uuid::new_v4();
        let did = Uuid::new_v4();

        store.add(make_pending(did, uid, 100)).await;
        store.remove(did).await;

        assert!(store.pending_for_user(uid).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn multiple_debits_per_user() {
        let store = make_store();
        let uid = Uuid::new_v4();
        let did1 = Uuid::new_v4();
        let did2 = Uuid::new_v4();

        store.add(make_pending(did1, uid, 50)).await;
        store.add(make_pending(did2, uid, 75)).await;

        assert_eq!(store.pending_for_user(uid).await.unwrap().len(), 2);

        store.remove(did1).await;
        let remaining = store.pending_for_user(uid).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].debit_id, did2);
    }
}
