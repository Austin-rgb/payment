//! Binary entry-point.
//!
//! Wires together all components and starts:
//! 1. The SQLite connection pool + migrations.
//! 2. The Actix-web HTTP server.
//! 3. A background Tokio task that drains the Moka event store and logs each
//!    event (replace the log call with a real broker publish in production).

use std::sync::Arc;
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tracing::info;
use tracing_subscriber::EnvFilter;
pub mod event_store;
pub mod handlers;
pub mod models;
pub mod pending_store;
pub mod repository;
pub mod service;
pub mod subscriptions;

use crate::{
    handlers::configure,
    event_store::MokaEventStore,
    handlers::AppState,
    pending_store::MokaPendingDebitStore,
    repository::SqliteRepository,
    service::PaymentService,
event_store::EventStore,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// SQLite database URL.  Override with the `DATABASE_URL` env var.
const DEFAULT_DB_URL: &str = "sqlite://payment.db?mode=rwc";

/// How long to retain a pending debit before emitting `payment.debit.failed`.
const MAXIMUM_WAIT_SECS: i64 = 300; // 5 minutes

/// Moka cache capacity (entries) for pending debits and outbox events.
const CACHE_CAPACITY: u64 = 10_000;

/// How often the background worker drains the event store.
const DRAIN_INTERVAL_MS: u64 = 500;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // Initialise tracing from `RUST_LOG` (default: `info`).
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    // ------------------------------------------------------------------
    // Database pool + migrations
    // ------------------------------------------------------------------
    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB_URL.to_owned());

    let opts = db_url.parse::<SqliteConnectOptions>()?;
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(opts)
        .await?;

    
    info!("database migrations complete");

    // ------------------------------------------------------------------
    // Shared service components
    // ------------------------------------------------------------------
    let repo    = Arc::new(SqliteRepository::new(pool));
    let pending = Arc::new(MokaPendingDebitStore::new(CACHE_CAPACITY));
    let events  = Arc::new(MokaEventStore::new(CACHE_CAPACITY));

    let service = Arc::new(PaymentService::new(
        repo,
        pending,
        events.clone(),
        chrono::Duration::seconds(MAXIMUM_WAIT_SECS),
    ));

    // ------------------------------------------------------------------
    // Background event-drain worker
    // ------------------------------------------------------------------
    // In production, replace the `info!` call below with a call to your
    // broker's publish method (e.g. NATS, Redis Streams, Kafka).
    let drain_events = events.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(DRAIN_INTERVAL_MS);
        loop {
            tokio::time::sleep(interval).await;
            for event in drain_events.drain().await {
                info!(
                    event_id = %event.id,
                    topic    = %event.topic,
                    payload  = %event.payload,
                    "would publish event to broker"
                );
                // broker.publish(&event.topic, &event.payload).await;
            }
        }
    });

    // ------------------------------------------------------------------
    // Actix-web HTTP server
    // ------------------------------------------------------------------
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    info!(addr = %bind_addr, "starting HTTP server");

    let svc = service.clone();
    HttpServer::new(move || {
        let state = web::Data::new(AppState {
            service: svc.clone(),
        });

        App::new()
            .app_data(state)
            .configure(configure::<
                SqliteRepository,
                MokaPendingDebitStore,
                MokaEventStore,
            >)
    })
    .bind(&bind_addr)?
    .run()
    .await?;

    Ok(())
}

