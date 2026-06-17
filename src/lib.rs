//! Payment service library crate.
//!
//! # Module layout
//!
//! | Module             | Contents                                              |
//! |--------------------|-------------------------------------------------------|
//! | [`models`]         | All public data structs and enums                     |
//! | [`schema`]         | SQLite DDL constants and the migration helper         |
//! | [`repository`]     | [`PaymentRepository`] trait + SQLite implementation   |
//! | [`pending_store`]  | [`PendingDebitStore`] trait + Moka implementation     |
//! | [`event_store`]    | [`EventStore`] trait + Moka implementation (outbox)   |
//! | [`service`]        | [`PaymentService`] — core business logic              |
//! | [`handlers`]       | Actix-web HTTP handlers and route config              |
//! | [`subscriptions`]  | Broker traits and subscription wiring                 |

pub mod handlers;
pub mod models;
pub mod pending_store;
pub mod repository;
pub mod service;
pub mod subscriptions;
use std::sync::Arc;

use actix_web::web::ServiceConfig;
use event_stream::EventStream;
pub use handlers::{AppState, configure};
pub use models::{
    CreditRequest, DebitFailed, DebitFailureReason, DebitRequest, DebitSuccess, PendingDebit,
    PendingEvent,
};
pub use pending_store::{MokaPendingDebitStore, PendingDebitStore};
pub use repository::{PaymentRepository, SqliteRepository};
pub use service::PaymentService;

pub use subscriptions::register_subscriptions;

pub struct Module {
    state: AppState<SqliteRepository, MokaPendingDebitStore>,
}

impl Module {
    pub fn new(pool: sqlx::Pool<sqlx::Sqlite>, es: Arc<dyn EventStream>) -> Self {
        let repo = Arc::new(SqliteRepository::new(pool));
        let pending = Arc::new(MokaPendingDebitStore::new(1000));
        let svc = Arc::new(PaymentService::new(
            repo,
            pending,
            es,
            chrono::Duration::minutes(30),
        ));
        let state = AppState { service: svc };
        Self { state }
    }

    pub fn config(self, cfg: &mut ServiceConfig) {
        cfg.app_data(Arc::new(self.state))
            .configure(configure::<SqliteRepository, MokaPendingDebitStore>);
    }
}
