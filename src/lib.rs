//! Payment service library crate.
//!
//! # Module layout
//!
//! | Module             | Contents                                                |
//! |---------------------|---------------------------------------------------------|
//! | [`models`]          | All public data structs and enums                       |
//! | [`repository`]      | [`repository::PaymentRepository`] trait + Postgres impl |
//! | [`pending_store`]   | [`pending_store::PendingDebitStore`] trait + cache-factory-backed impl |
//! | [`admin`]           | Plain CRUD viewsets exposed under `/admin/*`             |
//! | [`service`]         | [`service::PaymentService`] — core business logic        |
//! | [`handlers`]        | Actix-web HTTP handlers and route config                 |
//! | [`subscriptions`]   | Broker traits and subscription wiring                    |
//!
//! ## Cache decoupling
//!
//! Like `contributions`, this crate never constructs a cache backend (e.g.
//! [`moka`]) directly inside its business logic. Instead every cache —
//! the pending-debit store's two internal caches (see [`pending_store`]) and
//! the three admin viewsets' item/list caches (see [`admin`]) — is obtained
//! from a [`CacheFactory`] passed into [`Module::new`]. This crate defines
//! its own `CacheFactory` trait (rather than reusing `groups::CacheFactory`
//! as `contributions` does) since, unlike `contributions`, this service does
//! not nest another crate's module and so has no cross-crate cache-sharing
//! requirement — [`MokaCacheFactory`] below is this crate's own in-process,
//! [`moka`]-backed default implementation, analogous to `contributions`'
//! binary-local `SharedCacheFactory`.
//!
//! ## Admin auth
//!
//! [`AuthSession`] is this crate's own platform-wide identity session,
//! field-for-field identical in shape to `contributions`' (and `groups`')
//! own — all three decode the same underlying session payload, as
//! independent services trusting the same auth provider would. It gates
//! only the `/admin/*` scope (see [`Module::config`]); the pre-existing
//! `/payment/credit` and `/payment/statement` routes are left exactly as
//! they were (no session requirement), since they are called by other
//! internal systems rather than end users.
mod admin;
mod errors;
mod handlers;
mod models;
mod pending_store;
mod repository;
mod service;
mod subscriptions;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use actix_web::web::{ServiceConfig, scope};
use actixutils::Store;
use actixutils::middleware::{PermissionSet, Permissions, Principal, RequiredSession};
use moka::future::Cache;
use sqlx::{Pool, Postgres};
use typed_eventbus::EventStream;
use uuid::Uuid;
use viewset::ViewSet;

use admin::{
    AccountRepository, CreditRepository, ProcessedDebitRepository, create_account_viewset,
    create_credit_viewset, create_processed_debit_viewset,
};
use handlers::{AppState, configure};
use pending_store::CacheBackedPendingDebitStore;
use repository::PostgresRepository;
use service::PaymentService;
use subscriptions::register_subscriptions;

// ---------------------------------------------------------------------------
// Cache factory
// ---------------------------------------------------------------------------

/// Supplies named, typed caches to this crate's stores and repositories.
///
/// Implementations decide the storage backend (in-process, Redis, etc.) and
/// whatever sharing/eviction policy makes sense for it; callers only see the
/// [`actixutils::Store`] trait object.
///
/// This used to be its own copy of the trait (see the module-level docs'
/// original reasoning about not reusing `groups::CacheFactory`) — but it was
/// structurally identical to [`actixutils::locals::CacheFactory`] and
/// nominally distinct from it, so a `redis::aio::ConnectionManager` (which
/// implements the `actixutils` trait, see its `redis` feature) couldn't be
/// passed in directly. Re-exporting the `actixutils` trait instead means
/// `main.rs` can hand `Module::new` the `ConnectionManager` from
/// `infra::Infrastructure` with no adapter.
pub use actixutils::locals::CacheFactory;

/// [`CacheFactory`] backed by in-process [`moka`] caches, keyed by `name` so
/// that repeated calls for the same logical cache name return the *same*
/// underlying cache instead of a fresh, unshared one each time.
///
/// This crate has no cross-crate cache-sharing requirement (unlike
/// `contributions`/`groups`' shared `"membership_items"` cache), but the
/// name-keyed registry is kept anyway: it's what makes it safe for
/// [`Module::new`] to be called more than once against the same factory
/// instance (e.g. in tests) without silently doubling memory per cache.
///
/// Note: `_ttl` is ignored — every cache is created with a fixed max
/// capacity of 1000 entries and no expiry policy the first time its name is
/// seen. Entries are evicted only by `moka`'s capacity-based (LRU-ish)
/// policy once a given cache is full.
#[derive(Clone, Default)]
pub struct MokaCacheFactory {
    registry: Arc<Mutex<HashMap<String, Box<dyn Any + Send + Sync>>>>,
}

impl CacheFactory for MokaCacheFactory {
    // Bounds match `actixutils::locals::CacheFactory::new_cache` exactly
    // (Serialize/DeserializeOwned included) even though this in-process
    // moka cache never (de)serializes anything, so every K/V usable with
    // the Redis-backed factory in production is also usable here in tests.
    fn new_cache<K, V>(&self, name: &str, _ttl: Duration) -> Arc<dyn Store<K, V>>
    where
        K: Hash + Eq + Clone + serde::Serialize + Send + Sync + 'static,
        V: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        let mut registry = self.registry.lock().expect("cache registry lock poisoned");

        if let Some(existing) = registry.get(name) {
            match existing.downcast_ref::<Arc<dyn Store<K, V>>>() {
                Some(cache) => return cache.clone(),
                None => {
                    // Same cache name requested with a different (K, V) pair.
                    // This is a wiring bug, but we'd rather log and hand back
                    // an unshared cache than take the whole process down.
                    tracing::error!(
                        "cache '{name}' was previously created with a different key/value type; \
                         returning an unshared cache instead of sharing"
                    );
                    let cache: Cache<K, V> = Cache::new(1000);
                    return Arc::new(cache);
                }
            }
        }

        let cache: Cache<K, V> = Cache::new(1000);
        let store: Arc<dyn Store<K, V>> = Arc::new(cache);
        registry.insert(name.to_string(), Box::new(store.clone()));
        store
    }
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// This crate's own platform-wide identity, resolved from the top-level
/// `session` cookie by `RequiredSession::<AuthSession>` in
/// [`Module::config`]. Field-for-field identical to `contributions`' (and
/// `groups`') own (private) `AuthSession`.
#[derive(Clone, Serialize, Deserialize)]
struct AuthSession {
    /// The authenticated user's id ("subject").
    #[allow(dead_code)]
    pub sub: Uuid,
    /// Platform-wide permission bitmask, used only for the `/admin/*` scope.
    pub role: u128,
}

impl Principal for AuthSession {
    fn role(&self) -> u128 {
        self.role
    }
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

/// The Payment feature module: owns the [`PaymentService`] (built on
/// [`PostgresRepository`] and [`CacheBackedPendingDebitStore`]) plus the
/// three admin repositories, and wires everything into an `actix-web`
/// [`ServiceConfig`].
///
/// Construct one with [`Module::new`] and mount it with [`Module::config`]
/// inside `App::new().configure(...)`.
// `+ Clone` is explicit here now: it used to come from the (removed) local
// `CacheFactory: Clone + Send + Sync + 'static` supertrait —
// `actixutils::locals::CacheFactory` requires neither `Clone` nor
// `Send + Sync + 'static`, and `Module::new` below does `cf.clone()`
// three times and stores `cf` across await points.
pub struct Module<Cf: CacheFactory + Clone + Send + Sync + 'static> {
    state: Arc<AppState<PostgresRepository, CacheBackedPendingDebitStore>>,
    accounts: Arc<AccountRepository>,
    credits: Arc<CreditRepository>,
    processed_debits: Arc<ProcessedDebitRepository>,
    cf: Cf,
    /// Permission set for this crate's `/admin/*` scope, evaluated against
    /// [`AuthSession::role`]. Loaded from `payment_admin_permissions.json`
    /// by the caller (see `main.rs`).
    permissions: PermissionSet,
}

impl<Cf: CacheFactory + Clone + Send + Sync + 'static> Module<Cf> {
    /// Builds a new [`Module`]:
    /// - constructs [`PostgresRepository`] and a [`CacheBackedPendingDebitStore`]
    ///   (via `cf`) and wires them into a [`PaymentService`] with a 30-minute
    ///   `maximum_wait`;
    /// - subscribes that service to `payment.debit.request` on `es`;
    /// - constructs the three admin repositories (via `cf`).
    ///
    /// `permissions` is this crate's platform-wide permission set, applied
    /// to its `/admin/*` scope.
    pub async fn new(
        pool: Pool<Postgres>,
        cf: Cf,
        es: Arc<dyn EventStream>,
        permissions: PermissionSet,
    ) -> Self {
        let repo = Arc::new(PostgresRepository::new(pool.clone()));
        let pending = Arc::new(CacheBackedPendingDebitStore::new(
            &cf,
            Duration::from_mins(60),
        ));
        let svc = Arc::new(PaymentService::new(
            repo,
            pending,
            es.clone(),
            chrono::Duration::minutes(30),
        ));
        register_subscriptions(svc.clone(), es).await;

        let accounts = Arc::new(AccountRepository::new(pool.clone(), cf.clone()));
        let credits = Arc::new(CreditRepository::new(pool.clone(), cf.clone()));
        let processed_debits = Arc::new(ProcessedDebitRepository::new(pool, cf.clone()));

        Self {
            state: Arc::new(AppState { service: svc }),
            accounts,
            credits,
            processed_debits,
            cf,
            permissions,
        }
    }

    /// Registers every route owned by this module onto `cfg`.
    ///
    /// The resulting service tree is:
    ///
    /// ```text
    /// /payment/credit                 POST   (no session required)
    /// /payment/statement/{user_id}    GET    (no session required)
    /// /admin/*                        (requires this crate's own AuthSession + admin PermissionSet)
    /// ├── /admin/accounts/...           generic CRUD viewset over accounts
    /// ├── /admin/credits/...            generic CRUD viewset over credits
    /// └── /admin/processed-debits/...   generic CRUD viewset over processed debits
    /// ```
    pub fn config(&self, cfg: &mut ServiceConfig) {
        cfg.app_data(self.state.clone())
            .configure(configure::<PostgresRepository, CacheBackedPendingDebitStore>)
            .service(
                scope("admin")
                    .wrap(RequiredSession::<AuthSession>::new(
                        self.cf
                            .new_cache("payment_session_items", Duration::from_mins(60)),
                    ))
                    .wrap(Permissions::<AuthSession>::new(self.permissions.clone()))
                    .configure(|cfg| {
                        create_account_viewset(self.accounts.clone()).configure(cfg, "accounts")
                    })
                    .configure(|cfg| {
                        create_credit_viewset(self.credits.clone()).configure(cfg, "credits")
                    })
                    .configure(|cfg| {
                        create_processed_debit_viewset(self.processed_debits.clone())
                            .configure(cfg, "processed-debits")
                    }),
            );
    }
}
