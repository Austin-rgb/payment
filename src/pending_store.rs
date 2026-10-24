//! Cache-backed store for debits that are waiting for sufficient funds.
//!
//! Pending debits are intentionally **not** durable (see
//! [`crate::models::PendingDebit`]). Two ferrumec [`Store`]s are used so a
//! per-user lookup never needs a scan:
//!
//! ```text
//! pending_debits:           debit_id -> PendingDebit
//! pending_debit_user_index: user_id  -> Vec<debit_id>
//! ```
//!
//! Both come from the infra's [`CacheFactory`]: with `LocalInfra` they are
//! per-process Moka caches, with `RemoteInfra` they are shared in Redis, so
//! replicas see the same parked debits. The user-index update is a
//! read-modify-write, so two replicas parking debits for the *same user* at the
//! same instant can lose one index entry; that debit is then only noticed when
//! it is next parked or expires from the cache.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ferrumec::{CacheFactory, Store};
use uuid::Uuid;
use viewset::ApiError;

use crate::models::PendingDebit;

type Result<T> = std::result::Result<T, ApiError>;

fn cache_err(e: Box<dyn Error>) -> ApiError {
    ApiError::Internal(format!("pending-debit cache: {e}"))
}

#[async_trait]
pub trait PendingDebitStore: Send + Sync + 'static {
    /// Park a debit for deferred retry. Parking an id that is already parked
    /// keeps the original deadline (a redelivered request must not extend it).
    async fn add(&self, pending: PendingDebit) -> Result<()>;

    /// Remove a debit (after success or expiry). Removing a missing id is fine.
    async fn remove(&self, debit_id: Uuid) -> Result<()>;

    /// All pending debits currently associated with `user_id`.
    async fn pending_for_user(&self, user_id: Uuid) -> Result<Vec<PendingDebit>>;
}

pub struct CachePendingDebitStore {
    debits: Arc<dyn Store<Uuid, PendingDebit>>,
    user_index: Arc<dyn Store<Uuid, Vec<Uuid>>>,
}

impl CachePendingDebitStore {
    /// `ttl` is only a backstop for entries nobody revisits; the real deadline
    /// is [`PendingDebit::expires_at`]. Keep it comfortably above the
    /// service's maximum wait, or a debit can be evicted before it expires
    /// (and then vanishes without a `DebitFailed` event).
    pub fn new(cf: &impl CacheFactory, ttl: Duration) -> Self {
        Self {
            debits: cf.new_cache("pending_debits", ttl),
            user_index: cf.new_cache("pending_debit_user_index", ttl),
        }
    }
}

#[async_trait]
impl PendingDebitStore for CachePendingDebitStore {
    async fn add(&self, pending: PendingDebit) -> Result<()> {
        let debit_id = pending.debit_id;
        let user_id = pending.user_id;

        let existing = self.debits.get(&debit_id).await.map_err(cache_err)?;
        if existing.is_some() {
            return Ok(());
        }
        self.debits
            .set(&debit_id, pending)
            .await
            .map_err(cache_err)?;

        let ids = self.user_index.get(&user_id).await.map_err(cache_err)?;
        let mut ids = ids.unwrap_or_default();
        if !ids.contains(&debit_id) {
            ids.push(debit_id);
        }
        self.user_index
            .set(&user_id, ids)
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    async fn remove(&self, debit_id: Uuid) -> Result<()> {
        let pending = self.debits.get(&debit_id).await.map_err(cache_err)?;
        let Some(pending) = pending else {
            return Ok(());
        };
        let user_id = pending.user_id;
        self.debits.delete(&debit_id).await.map_err(cache_err)?;

        let ids = self.user_index.get(&user_id).await.map_err(cache_err)?;
        if let Some(mut ids) = ids {
            ids.retain(|id| *id != debit_id);
            if ids.is_empty() {
                self.user_index.delete(&user_id).await.map_err(cache_err)?;
            } else {
                self.user_index
                    .set(&user_id, ids)
                    .await
                    .map_err(cache_err)?;
            }
        }
        Ok(())
    }

    async fn pending_for_user(&self, user_id: Uuid) -> Result<Vec<PendingDebit>> {
        let ids = self.user_index.get(&user_id).await.map_err(cache_err)?;
        let Some(ids) = ids else {
            return Ok(vec![]);
        };

        let mut result = Vec::with_capacity(ids.len());
        for id in &ids {
            let pending = self.debits.get(id).await.map_err(cache_err)?;
            if let Some(pending) = pending {
                result.push(pending);
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use ferrumec::cache::MokaCacheFactory;

    fn pending(debit_id: Uuid, user_id: Uuid, amount: u64) -> PendingDebit {
        PendingDebit {
            debit_id,
            order_id: Uuid::new_v4(),
            user_id,
            amount,
            expires_at: Utc::now() + chrono::Duration::seconds(60),
        }
    }

    fn store() -> CachePendingDebitStore {
        CachePendingDebitStore::new(&MokaCacheFactory::default(), Duration::from_secs(3600))
    }

    #[tokio::test]
    async fn add_and_lookup() {
        let store = store();
        let (uid, did) = (Uuid::new_v4(), Uuid::new_v4());

        store.add(pending(did, uid, 100)).await.unwrap();

        let found = store.pending_for_user(uid).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].debit_id, did);
    }

    #[tokio::test]
    async fn remove_cleans_index() {
        let store = store();
        let (uid, did) = (Uuid::new_v4(), Uuid::new_v4());

        store.add(pending(did, uid, 100)).await.unwrap();
        store.remove(did).await.unwrap();

        assert!(store.pending_for_user(uid).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn multiple_debits_per_user() {
        let store = store();
        let uid = Uuid::new_v4();
        let (d1, d2) = (Uuid::new_v4(), Uuid::new_v4());

        store.add(pending(d1, uid, 50)).await.unwrap();
        store.add(pending(d2, uid, 75)).await.unwrap();
        assert_eq!(store.pending_for_user(uid).await.unwrap().len(), 2);

        store.remove(d1).await.unwrap();
        let remaining = store.pending_for_user(uid).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].debit_id, d2);
    }

    #[tokio::test]
    async fn re_adding_keeps_original_deadline() {
        let store = store();
        let (uid, did) = (Uuid::new_v4(), Uuid::new_v4());

        let first = pending(did, uid, 100);
        let deadline = first.expires_at;
        store.add(first).await.unwrap();

        let mut again = pending(did, uid, 100);
        again.expires_at = deadline + chrono::Duration::minutes(30);
        store.add(again).await.unwrap();

        let found = store.pending_for_user(uid).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].expires_at, deadline);
    }
}
