//! Generic CRUD viewsets exposed under `/admin/*` for platform administrators.
//!
//! Mirrors `contributions::admin`: each entity here (`Account`, `Credit`,
//! `ProcessedDebit`) is a `viewset::Entity`-derived struct backed by a tiny
//! repository that hands `sqlx::query_as!`-free generic CRUD off to
//! `viewset::DefaultViewSet`. These are deliberately **separate** from
//! [`crate::repository::PostgresRepository`] (which implements the
//! hand-rolled [`crate::repository::PaymentRepository`] business-logic
//! trait): the two access the same three tables, but the admin entities
//! exist purely so platform administrators can browse/correct raw rows —
//! they bypass `PaymentService` entirely and do not publish domain events or
//! touch pending-debit retry logic.
//!
//! Access is gated purely by the top-level [`crate::AuthSession`] permission
//! set (see [`crate::Module::config`]) — there is no per-user scoping here,
//! so anyone with platform admin rights can act on any user's accounts,
//! credits, and processed debits.
use crate::CacheFactory;
use actixutils::Store;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Pool, Postgres};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use viewset::{DefaultViewSet, Entity, Repository, Service};

// ============ Account ============

/// A user's balance row (table `accounts`).
///
/// `id` is the user's id directly — one row per user — rather than a
/// synthetic surrogate key, matching the underlying table (see
/// `migrations/20260917120000_account.sql`). It may be set on create (an
/// admin creating a zero-balance account ahead of a user's first credit)
/// but never changed afterwards.
#[derive(Entity, Debug, Clone, Serialize, Deserialize, FromRow)]
#[entity(create = "CreateAccount", update = "UpdateAccount")]
pub struct Account {
    #[entity(skip_update)]
    pub id: Uuid,
    pub balance: i64,
}

/// Payload for creating an [`Account`] from `/admin/accounts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAccount {
    pub id: Uuid,
    #[serde(default)]
    pub balance: i64,
}

/// Payload for updating an [`Account`] from `/admin/accounts`. Only
/// `balance` may change — an admin correcting a balance directly bypasses
/// the credit ledger, so use with care.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpdateAccount {
    pub balance: Option<i64>,
}

/// Data access for [`Account`], plus its item/list caches.
pub struct AccountRepository {
    pub pool: Pool<Postgres>,
    pub item_cache: Arc<dyn Store<Uuid, Account>>,
    pub list_cache: Arc<dyn Store<u64, (Vec<Account>, i64)>>,
}

impl AccountRepository {
    /// Builds a new repository, creating its caches via `cf`.
    pub fn new<Cf: CacheFactory + 'static>(pool: Pool<Postgres>, cf: Cf) -> Self {
        Self {
            pool,
            item_cache: cf.new_cache("account_items", Duration::from_mins(60)),
            list_cache: cf.new_cache("account_lists", Duration::from_mins(30)),
        }
    }
}

impl Repository for AccountRepository {
    type Entity = Account;
    fn list_cache(&self) -> Arc<dyn Store<u64, (Vec<Account>, i64)> + Send + Sync> {
        self.list_cache.clone()
    }
    fn cache(&self) -> Arc<dyn Store<Uuid, Account> + Send + Sync> {
        self.item_cache.clone()
    }
    fn database(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

/// Adapts [`AccountRepository`] for [`viewset::DefaultViewSet`].
pub(crate) struct AccountService {
    repo: Arc<AccountRepository>,
}

impl Service for AccountService {
    type Repository = AccountRepository;
    fn repository(&self) -> &AccountRepository {
        &self.repo
    }
}

/// Builds the generic admin CRUD viewset over accounts, mounted at
/// `/admin/accounts` by [`crate::Module::config`].
pub fn create_account_viewset(repo: Arc<AccountRepository>) -> Arc<DefaultViewSet<AccountService>> {
    Arc::new(AccountService { repo }.into())
}

// ============ Credit ============

/// An append-only credit ledger row (table `credits`).
///
/// Credits are immutable once created — there is no `update` — mirroring
/// `contributions::submission::Submission`.
#[derive(Entity, Debug, Clone, Serialize, Deserialize, FromRow)]
#[entity(create = "CreateCredit")]
pub struct Credit {
    #[entity(skip_create)]
    pub id: Uuid,
    pub user_id: Uuid,
    pub amount: i64,
    #[entity(skip_create)]
    pub created: DateTime<Utc>,
}

/// Payload for creating a [`Credit`] from `/admin/credits`.
///
/// Note this bypasses [`crate::service::PaymentService::credit`] entirely:
/// it inserts a ledger row without also updating `accounts.balance` or
/// retrying pending debits. It exists for administrative record-correction,
/// not as a substitute for `POST /payment/credit`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCredit {
    pub user_id: Uuid,
    pub amount: i64,
}

/// Data access for [`Credit`], plus its item/list caches.
pub struct CreditRepository {
    pub pool: Pool<Postgres>,
    pub item_cache: Arc<dyn Store<Uuid, Credit>>,
    pub list_cache: Arc<dyn Store<u64, (Vec<Credit>, i64)>>,
}

impl CreditRepository {
    /// Builds a new repository, creating its caches via `cf`.
    pub fn new<Cf: CacheFactory + 'static>(pool: Pool<Postgres>, cf: Cf) -> Self {
        Self {
            pool,
            item_cache: cf.new_cache("credit_items", Duration::from_mins(60)),
            list_cache: cf.new_cache("credit_lists", Duration::from_mins(30)),
        }
    }
}

impl Repository for CreditRepository {
    type Entity = Credit;
    fn list_cache(&self) -> Arc<dyn Store<u64, (Vec<Credit>, i64)> + Send + Sync> {
        self.list_cache.clone()
    }
    fn cache(&self) -> Arc<dyn Store<Uuid, Credit> + Send + Sync> {
        self.item_cache.clone()
    }
    fn database(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

/// Adapts [`CreditRepository`] for [`viewset::DefaultViewSet`].
pub(crate) struct CreditService {
    repo: Arc<CreditRepository>,
}

impl Service for CreditService {
    type Repository = CreditRepository;
    fn repository(&self) -> &CreditRepository {
        &self.repo
    }
}

/// Builds the generic admin CRUD viewset over credits, mounted at
/// `/admin/credits` by [`crate::Module::config`].
pub fn create_credit_viewset(repo: Arc<CreditRepository>) -> Arc<DefaultViewSet<CreditService>> {
    Arc::new(CreditService { repo }.into())
}

// ============ ProcessedDebit ============

/// A settled-debit idempotency record (table `processed_debits`).
///
/// `id` is the caller-supplied `debit_id`, not server-generated — this
/// mirrors the domain's idempotency-key semantics, so unlike [`Account`]'s
/// `id` this one is still eligible for `skip_create` since it is *always*
/// supplied by the client on create rather than defaulted; `skip_create`
/// here instead reflects that these rows are immutable/no `update` route
/// exists, matching `contributions::submission::Submission`.
#[derive(Entity, Debug, Clone, Serialize, Deserialize, FromRow)]
#[entity(create = "CreateProcessedDebit")]
pub struct ProcessedDebit {
    pub id: Uuid,
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub amount: i64,
    #[entity(skip_create)]
    pub created: DateTime<Utc>,
}

/// Payload for creating a [`ProcessedDebit`] from `/admin/processed-debits`.
///
/// As with [`CreateCredit`], this bypasses
/// [`crate::service::PaymentService::handle_debit_request`] entirely: it
/// does not deduct `accounts.balance` or publish `payment.debit.success`. It
/// exists for administrative record-correction (e.g. manually reconciling
/// after an out-of-band debit), not as a substitute for the normal debit
/// flow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateProcessedDebit {
    pub id: Uuid,
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub amount: i64,
}

/// Data access for [`ProcessedDebit`], plus its item/list caches.
pub struct ProcessedDebitRepository {
    pub pool: Pool<Postgres>,
    pub item_cache: Arc<dyn Store<Uuid, ProcessedDebit>>,
    pub list_cache: Arc<dyn Store<u64, (Vec<ProcessedDebit>, i64)>>,
}

impl ProcessedDebitRepository {
    /// Builds a new repository, creating its caches via `cf`.
    pub fn new<Cf: CacheFactory + 'static>(pool: Pool<Postgres>, cf: Cf) -> Self {
        Self {
            pool,
            item_cache: cf.new_cache("processed_debit_items", Duration::from_mins(60)),
            list_cache: cf.new_cache("processed_debit_lists", Duration::from_mins(30)),
        }
    }
}

impl Repository for ProcessedDebitRepository {
    type Entity = ProcessedDebit;
    fn list_cache(&self) -> Arc<dyn Store<u64, (Vec<ProcessedDebit>, i64)> + Send + Sync> {
        self.list_cache.clone()
    }
    fn cache(&self) -> Arc<dyn Store<Uuid, ProcessedDebit> + Send + Sync> {
        self.item_cache.clone()
    }
    fn database(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

/// Adapts [`ProcessedDebitRepository`] for [`viewset::DefaultViewSet`].
pub(crate) struct ProcessedDebitService {
    repo: Arc<ProcessedDebitRepository>,
}

impl Service for ProcessedDebitService {
    type Repository = ProcessedDebitRepository;
    fn repository(&self) -> &ProcessedDebitRepository {
        &self.repo
    }
}

/// Builds the generic admin CRUD viewset over processed debits, mounted at
/// `/admin/processed-debits` by [`crate::Module::config`].
pub fn create_processed_debit_viewset(
    repo: Arc<ProcessedDebitRepository>,
) -> Arc<DefaultViewSet<ProcessedDebitService>> {
    Arc::new(ProcessedDebitService { repo }.into())
}
