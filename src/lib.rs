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

mod handlers;
mod models;
mod pending_store;
mod repository;
mod service;
mod subscriptions;
use std::sync::Arc;

use actix_web::web::ServiceConfig;
use event_stream::EventStream;
use handlers::{AppState, configure};

use pending_store::MokaPendingDebitStore;
use repository::SqliteRepository;
use service::PaymentService;

use subscriptions::register_subscriptions;

pub struct Module {
    state: AppState<SqliteRepository, MokaPendingDebitStore>,
}

impl Module {
    pub async fn new(pool: sqlx::Pool<sqlx::Sqlite>, es: Arc<dyn EventStream>) -> Self {
        let repo = Arc::new(SqliteRepository::new(pool));
        let pending = Arc::new(MokaPendingDebitStore::new(1000));
        let svc = Arc::new(PaymentService::new(
            repo,
            pending,
            es.clone(),
            chrono::Duration::minutes(30),
        ));
        let state = AppState {
            service: svc.clone(),
        };
        register_subscriptions(svc.clone(), es).await;
        Self { state }
    }

    pub fn config(self, cfg: &mut ServiceConfig) {
        cfg.app_data(Arc::new(self.state))
            .configure(configure::<SqliteRepository, MokaPendingDebitStore>);
    }
}

