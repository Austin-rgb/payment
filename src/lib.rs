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

//pub mod event_store;
pub mod handlers;
pub mod models;
pub mod pending_store;
pub mod repository;
pub mod service;
pub mod subscriptions;
pub mod event_store;
// Convenience re-exports for the most commonly referenced types.
//pub use event_store::{EventStore, MokaEventStore};
pub use handlers::{configure, AppState};
pub use models::{
    CreditRequest, DebitFailed, DebitFailureReason, DebitRequest, DebitSuccess, PendingDebit,
    PendingEvent,
};
pub use pending_store::{MokaPendingDebitStore, PendingDebitStore};
pub use repository::{PaymentRepository, SqliteRepository};
pub use service::PaymentService;
pub use subscriptions::{register_subscriptions};
