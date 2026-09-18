//! Binary entry point: wires up the Postgres pool, an in-process
//! [`MokaCacheFactory`], a local (in-process) [`typed_eventbus`] event
//! stream, and the platform-admin `PermissionSet` for `/admin/*`, then
//! serves [`Module`] on `127.0.0.1:8081`.
//!
//! This is the "default" wiring for running Payment as a standalone
//! microservice: everything here is in-process (no external cache, no
//! external broker) other than Postgres itself. Swap
//! [`MokaCacheFactory`] for a distributed [`CacheFactory`] implementation,
//! or [`typed_eventbus::LocalEventStream`] for
//! [`typed_eventbus::NatsEventStream`] (or another broker), to run this
//! behind multiple replicas without each holding independent state.
//!
//! ## Required environment / files
//!
//! - `DATABASE_URL` — Postgres connection string (loaded via `.env` if
//!   present, through `dotenvy`).
//! - `payment_admin_permissions.json` — permission set for this crate's own
//!   `/admin/*` scope.
//!
//! Migrations live under `migrations/` and are applied with
//! `sqlx migrate run` before this binary is started.
use actix_web::{App, HttpServer};
use actixutils::middleware::PermissionSet;
use dotenvy::dotenv;
use payment::{MokaCacheFactory, Module};
use sqlx::PgPool;
use std::sync::Arc;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use typed_eventbus::{EventStream, LocalEventStream};

/// Loads configuration, connects to Postgres, and serves [`Module`]'s HTTP
/// app on `127.0.0.1:8081`. Panics on missing `DATABASE_URL`, a failed DB
/// connection, or a missing/invalid admin permission file.
///
/// Migrations under `migrations/` are applied separately with
/// `sqlx migrate run` before this binary starts, exactly as `contributions`
/// documents for its own `main.rs`.
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let db_url = std::env::var("DATABASE_URL").expect("var DATABASE_URL not provided");
    let pool = PgPool::connect(&db_url)
        .await
        .expect("could not connect to db");

    let cache_factory = MokaCacheFactory::default();

    // In-process pub/sub. Every `payment.debit.request` publisher must run
    // in *this* process for this to see it — swap in
    // `typed_eventbus::NatsEventStream` (or another broker) to fan out
    // across replicas/processes.
    let event_stream: Arc<dyn EventStream> = Arc::new(LocalEventStream::new(1024));

    let admin_permissions = match PermissionSet::from_file("payment_admin_permissions.json") {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("failed to load payment admin permission set: {e}");
            panic!()
        }
    };

    let module = Arc::new(
        Module::new(pool, cache_factory, event_stream, admin_permissions).await,
    );

    HttpServer::new(move || App::new().configure(|cfg| module.clone().config(cfg)))
        .bind(("127.0.0.1", 8081))?
        .run()
        .await
}
