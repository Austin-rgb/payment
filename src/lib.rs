//! Payment service, built on the `ferrumec` and `viewset` crates.
//!
//! | Module            | Contents                                                        |
//! |-------------------|-----------------------------------------------------------------|
//! | [`models`]        | HTTP bodies, statement types, domain events                     |
//! | [`ledger`]        | Postgres ledger: credit, atomic debit settlement, statement     |
//! | [`pending_store`] | Cache-backed store for debits waiting on funds                  |
//! | [`service`]       | [`service::PaymentService`] — business logic + outbox events    |
//! | [`subscriptions`] | `payment.debit.request` subscriber                              |
//! | [`handlers`]      | `POST …/credit`, `GET …/statement/{user_id}`                    |
//! | [`admin`]         | viewset CRUD over accounts / credits / processed debits         |
//! | [`module`]        | [`PaymentModule`], the `ferrumec::Module` that wires it all     |
//!
//! Infrastructure (Postgres pool, cache factory, event stream), the HTTP
//! server, authentication/permissions and observability all come from
//! ferrumec; this crate only supplies a [`ferrumec::Module`]. See `main.rs`
//! for the bootstrap and the README for permissions and URL layout.

pub mod admin;
pub mod handlers;
pub mod ledger;
pub mod models;
pub mod module;
pub mod pending_store;
pub mod service;
pub mod subscriptions;

pub use module::PaymentModule;
