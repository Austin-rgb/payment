//! Binary entry point: wires up Postgres and Redis via
//! [`infra::Infrastructure`], a local (in-process) [`typed_eventbus`] event
//! stream, and the platform-admin `PermissionSet` for `/admin/*`, then
//! serves [`Module`] on `BIND_ADDR` (default `127.0.0.1:8081`).
//!
//! Postgres and the cache are now real shared infrastructure (Redis, via
//! `infra.redis` — see [`payment::MokaCacheFactory`] if you need an
//! in-process cache instead, e.g. for tests). The event stream is still
//! in-process: swap [`typed_eventbus::LocalEventStream`] for
//! [`typed_eventbus::NatsEventStream::from_client`] (passing
//! `infra.nats.clone()`) to fan out across replicas without each holding
//! independent state.
//!
//! ## Required environment / files
//!
//! - `DATABASE_URL` / `REDIS_URL` — read by [`infra::Infrastructure::from_env`]
//!   (loaded via `.env` if present, through `dotenvy`).
//! - `payment_admin_permissions.json` — permission set for this crate's own
//!   `/admin/*` scope.
//!
//! Migrations live under `migrations/` and are applied with
//! `sqlx migrate run` before this binary is started.
use actix_web::{App, HttpServer};
use actixutils::middleware::PermissionSet;
use dotenvy::dotenv;
use infra::Infrastructure;
use payment::Module;
use std::sync::Arc;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use typed_eventbus::{EventStream, LocalEventStream};

/// Loads configuration, connects to Postgres and Redis, and serves
/// [`Module`]'s HTTP app on `BIND_ADDR` (default `127.0.0.1:8081`). Panics
/// on missing `DATABASE_URL`, a failed infrastructure connection, or a
/// missing/invalid admin permission file.
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

    let infra = Infrastructure::from_env()
        .await
        .expect("could not connect to infrastructure");
    let cache_factory = infra.redis.clone();

    // In-process pub/sub. Every `payment.debit.request` publisher must run
    // in *this* process for this to see it — swap in
    // `typed_eventbus::NatsEventStream::from_client(infra.nats.clone())`
    // (or another broker) to fan out across replicas/processes.
    let event_stream: Arc<dyn EventStream> = Arc::new(LocalEventStream::new(1024));

    let admin_permissions = match PermissionSet::from_file("payment_admin_permissions.json") {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("failed to load payment admin permission set: {e}");
            panic!()
        }
    };

    let module = Arc::new(
        Module::new(
            infra.postgres,
            cache_factory,
            event_stream,
            admin_permissions,
        )
        .await,
    );

    HttpServer::new(move || App::new().configure(|cfg| module.clone().config(cfg)))
        .bind(std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8081".into()))?
        .run()
        .await
}
