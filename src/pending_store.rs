//! In-memory store for debits that are waiting for sufficient funds.
//!
//! Pending debits are intentionally **not** durable. If the process restarts
//! while debits are pending those debits are lost silently — no `DebitFailed`
//! event is emitted. This is an explicit design trade-off: it keeps the
//! implementation free of distributed coordination while placing the
//! responsibility for timeout handling on the originating system.
//!
//! Two Moka caches are maintained to avoid a full-scan on per-user lookup:
//!
//! ```text
//! debits:     debit_id  →  PendingDebit
//! user_index: user_id   →  Vec<Uuid>   (debit IDs for that user)
//! ```

use crate::models::PendingDebit;

use async_trait::async_trait;
use moka::future::Cache;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait PendingDebitStore: Send + Sync + 'static {
    /// Park a debit for deferred retry.
    async fn add(&self, pending: PendingDebit);

    /// Remove a debit (called after success or expiry).
    async fn remove(&self, debit_id: Uuid);

    /// Return all pending debits currently associated with `user_id`.
    async fn pending_for_user(&self, user_id: Uuid) -> Vec<PendingDebit>;
}

// ---------------------------------------------------------------------------
// Moka implementation
// ---------------------------------------------------------------------------

/// [`PendingDebitStore`] backed by two Moka in-process caches.
pub struct MokaPendingDebitStore {
    /// Primary lookup: `debit_id → PendingDebit`.
    debits: Cache<Uuid, PendingDebit>,
    /// Secondary index: `user_id → Vec<debit_id>`.
    ///
    /// Maintained in sync with `debits` so that [`pending_for_user`] never
    /// needs to iterate all entries.
    user_index: Cache<Uuid, Vec<Uuid>>,
}

impl MokaPendingDebitStore {
    /// Create a new store with the given maximum entry capacity (applied to
    /// each internal cache independently).
    pub fn new(max_capacity: u64) -> Self {
        Self {
            debits:     Cache::new(max_capacity),
            user_index: Cache::new(max_capacity),
        }
    }
}

#[async_trait]
impl PendingDebitStore for MokaPendingDebitStore {
    async fn add(&self, pending: PendingDebit) {
        let debit_id = pending.debit_id;
        let user_id  = pending.user_id;

        self.debits.insert(debit_id, pending).await;

        // Append debit_id to the user's index list.
        let mut ids = self
            .user_index
            .get(&user_id)
            .await
            .unwrap_or_default();

        if !ids.contains(&debit_id) {
            ids.push(debit_id);
        }
        self.user_index.insert(user_id, ids).await;
    }

    async fn remove(&self, debit_id: Uuid) {
        // Look up the owning user before evicting from the primary cache.
        if let Some(pending) = self.debits.get(&debit_id).await {
            let user_id = pending.user_id;
            self.debits.invalidate(&debit_id).await;

            // Remove debit_id from the user's index list.
            if let Some(mut ids) = self.user_index.get(&user_id).await {
                ids.retain(|id| *id != debit_id);
                if ids.is_empty() {
                    self.user_index.invalidate(&user_id).await;
                } else {
                    self.user_index.insert(user_id, ids).await;
                }
            }
        }
    }

    async fn pending_for_user(&self, user_id: Uuid) -> Vec<PendingDebit> {
        let ids = match self.user_index.get(&user_id).await {
            Some(ids) => ids,
            None      => return vec![],
        };

        let mut result = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some(pending) = self.debits.get(id).await {
                result.push(pending);
            }
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_pending(debit_id: Uuid, user_id: Uuid, amount: u64) -> PendingDebit {
        PendingDebit {
            debit_id,
            user_id,
            amount,
            expires_at: Utc::now() + chrono::Duration::seconds(60),
        }
    }

    #[tokio::test]
    async fn add_and_lookup() {
        let store = MokaPendingDebitStore::new(128);
        let uid   = Uuid::new_v4();
        let did   = Uuid::new_v4();

        store.add(make_pending(did, uid, 100)).await;

        let pending = store.pending_for_user(uid).await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].debit_id, did);
    }

    #[tokio::test]
    async fn remove_cleans_index() {
        let store = MokaPendingDebitStore::new(128);
        let uid   = Uuid::new_v4();
        let did   = Uuid::new_v4();

        store.add(make_pending(did, uid, 100)).await;
        store.remove(did).await;

        assert!(store.pending_for_user(uid).await.is_empty());
    }

    #[tokio::test]
    async fn multiple_debits_per_user() -> Result<()> {
        let store = MokaPendingDebitStore::new(128);
        let uid   = Uuid::new_v4();
        let did1  = Uuid::new_v4();
        let did2  = Uuid::new_v4();

        store.add(make_pending(did1, uid, 50)).await;
        store.add(make_pending(did2, uid, 75)).await;

        assert_eq!(store.pending_for_user(uid).await.len(), 2);

        store.remove(did1).await;
        let remaining = store.pending_for_user(uid).await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].debit_id, did2);

        Ok(())
    }
}

